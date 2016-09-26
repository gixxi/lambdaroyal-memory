(ns lambdaroyal.memory.core.context
  (require [lambdaroyal.memory.core.tx :refer :all])
  (require [lambdaroyal.memory.eviction.core :refer [create-proxy]])
  (import [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint ReferencedIntegrityConstraint AttributeIndex])
  (:refer-clojure :exclude [find])
  (:gen-class))

(defn- ric-factory 
  "use this function to create a set of indexes for a single referential integrity constraint [ric]. This function might be used in order to dynamically enrich a meta modell by a new ric. the ric (map) might contain :name :coll :foreign-coll :foreign-key :name"
  [ric]
  (let [{:keys [coll name foreign-coll foreign-key unique]} ric
        unique (or unique false)
        name (or name (gensym))
        constraint (create-referrer-integrity-constraint name foreign-coll foreign-key)]
    (list  
      [coll constraint]
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [coll (create-attribute-index (gensym) unique [(.foreign-key constraint)])]
      ;;add reverse constraint - RIC on the parent/referenced collection
      [(.foreign-coll constraint) (create-referenced-integrity-constraint name coll (.foreign-key constraint))])))

(defn add-ric 
  "add a referential integrity constraint dynamically to a context [ctx]"
  [ctx ric]
  (dosync
   (doseq [x (ric-factory ric)]
     (let [[coll-name constraint] x
           ctx @ctx
           constraints (-> ctx coll-name :constraints)]
       (commute constraints assoc (.name constraint) constraint)
       ;;update and check indexes
       (if (contains? (.application constraint) :insert)
         (doseq [x (-> ctx coll-name :data deref)]
           (let [[k v] x]
             (.precommit constraint ctx coll-name :insert k v)
             (.postcommit constraint ctx coll-name :insert x))))))))

(defn remove-ric
  "removes all referential integrity constraints from the context [ctx] that refer the target collection [target] from the source collection [source]. The [foreign-key] within the source collection all the respective RICs need to match"
  [ctx source target foreign-key]
   (let [ctx @ctx
         source-coll (get ctx source)
         target-coll (get ctx target)] 
     (dosync
      ;;delete forward references
      (doseq [constraint (filter #(and 
                          (= (.foreign-coll %) target)
                          (= (.foreign-key %) foreign-key))
                                 (filter
                                  #(instance? ReferrerIntegrityConstraint %) (map last (-> source-coll :constraints deref))))]
        (commute (:constraints source-coll) dissoc (.name constraint)))
      ;;delete backward reference
      (doseq [constraint (filter #(and 
                          (= (.referencing-coll %) source)
                          (= (.referencing-key %) foreign-key))
                                 (filter
                                  #(instance? ReferencedIntegrityConstraint %) (map last (-> target-coll :constraints deref))))]
        (commute (:constraints target-coll) dissoc (.name constraint)))
      ;;delete index backing the ric
      (doseq [constraint (filter #(= (.attributes %) [foreign-key])
                                 (filter
                                  #(instance? AttributeIndex %) (map last (-> source-coll :constraints deref))))]
        (commute (:constraints source-coll) dissoc (.name constraint))))))

(defn referential-integrity-constraint-factory [meta-model]
  (reduce
   (fn [acc [coll unique constraint]]
     (conj 
      acc 
      [coll constraint]
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [coll (create-attribute-index (gensym) unique [(.foreign-key constraint)])]
      ;;add reverse constraint - RIC on the parent/referenced collection
      [(.foreign-coll constraint) (create-referenced-integrity-constraint (.name constraint) coll (.foreign-key constraint))]))
   []
   (map
    (fn [constraint]
      (let [{:keys [name coll foreign-coll foreign-key unique]} constraint
            name (or name (gensym))
            unique (or unique false)]
        [coll
         unique
         (create-referrer-integrity-constraint name foreign-coll foreign-key)]))
    (reduce (fn [acc [k v]]
              (concat acc (map #(assoc % :coll k) v))) []
              (zipmap (keys meta-model) (map :foreign-key-constraints (vals meta-model)))))))

(defn- create-collection [collection referential-integrity-constraints]
  (let [fn-constraint-factory 
        (fn [collection]
          ;;we assume all collections to be unique (CR 20150620)
          {:unique-key (create-unique-key-constraint)})
        fn-index-factory 
        (fn [collection]
          (reduce
           (fn [acc index]
             (let [name (or (:name index) (gensym))]
               (assoc acc 
                 name
                 (let [{:keys [unique attributes]} index]
                   (create-attribute-index name unique attributes)))))
           {}
           (:indexes collection)))]
    
    (#(if (:evictor collection) (assoc % :evictor (create-proxy (:evictor collection) (:evictor-delay collection))) %)
     {:running (ref (bigint 0))
      :name (:name collection)
      :data (ref (sorted-map))
      :constraints (ref 
                    (merge 
                     (fn-index-factory collection) 
                     (fn-constraint-factory collection) 
                     (reduce 
                      (fn [acc [coll constraint]]
                        (if 
                            (= (:name collection) coll)
                          (assoc acc (.name constraint) constraint)
                          acc))
                      {}
                      referential-integrity-constraints)))})))

(defn add-collection 
  "adds a collection with spec to the context [ctx]. returns the collection itself. Don't forget to call start on the respective evictor channel"
  [ctx coll]
  {:pre [(contains? coll :name) (not (contains? @ctx (:name coll))) ]}
  (let [coll' (create-collection coll (list))]
    (do
      (dosync
       (alter ctx assoc (:name coll) coll')
       (doseq [x (:foreign-key-constraints coll)]
         (add-ric ctx (assoc x :coll (:name coll)))))
      coll')))

(defn- get-referencing-colls
  "get all the names of collections that reference the collection with name [coll]"
  [ctx coll]
  (let [ctx @ctx
        coll' (get ctx coll)]
      (map #(.referencing-coll %)
           (filter
            #(instance? ReferencedIntegrityConstraint %) 
            (map last (-> coll' :constraints deref))))))

(defn- get-referenced-colls
  "get all the names of collections that are referenced by the collection with name [coll]"
  [ctx coll]
  (let [ctx @ctx
        coll' (get ctx coll)]
      (map #(.foreign-coll %)
           (filter
            #(instance? ReferrerIntegrityConstraint %) 
            (map last (-> coll' :constraints deref))))))

(defn- delete-referencing-constraints
  "delete all referencing constraints from a collection [source] to a target collection [target]"
  [ctx source target]
  (let [ctx @ctx
        source-coll (get ctx source)]
      (doseq [constraint (filter #(= (.foreign-coll %) target)
                                 (filter
                                  #(instance? ReferrerIntegrityConstraint %) (map last (-> source-coll :constraints deref))))]
        (commute (:constraints source-coll) dissoc (.name constraint)))))

(defn- delete-referenced-constraints
  "delete all referencing constraints from a collection [source] to a target collection [target]"
  [ctx source target]
  (let [ctx @ctx
        target-coll (get ctx target)]
      (doseq [constraint (filter #(= (.referencing-coll %) source)
                                 (filter
                                  #(instance? ReferencedIntegrityConstraint %) (map last (-> target-coll :constraints deref))))]
        (commute (:constraints target-coll) dissoc (.name constraint)))))

(defn delete-collection
  "removes a collection with name [coll]. All RICs of referencing colls will be deleted. All referencing colls are returned."
  [ctx coll]
  {:pre [(contains? @ctx coll) ]}
  (let [coll' (get @ctx coll)
        referenced-colls (get-referenced-colls ctx coll)
        referencing-colls (get-referencing-colls ctx coll)]
    (do
      (io!
       (dosync
        (doseq [referencing-coll referencing-colls]
          (delete-referencing-constraints ctx referencing-coll coll))
        (doseq [referenced-coll referenced-colls]
          (delete-referenced-constraints ctx coll referenced-coll))
        (alter ctx dissoc coll)))
      (if-let [evictor (:evictor coll')]
        (.delete-coll evictor coll))
      referencing-colls)))
  
(defn create-context [meta-model]
  (let [rics (referential-integrity-constraint-factory meta-model)]
    (ref (zipmap (keys meta-model) 
                 (map 
                  #(create-collection % rics) 
                  (map #(assoc %1 :name %2) (vals meta-model) (keys meta-model)))))))

(defn dependency-order 
  "returns a list of order names ordered by referential integrity constraints. So to say, if colls contains two collections a,b where a contains a referrer integrity constraint to b then a comes before b in the result."
  [[colls & _]]
  (let [stage (vec 
               (map first
                    (filter 
                     (fn [[k v]] 
                       (empty? v))
                     colls)))
        left (apply dissoc colls stage)
        left (reduce (fn [acc [k v]]
                       (assoc acc k (filter #(not (contains? (set stage) %)) v)))
                     {}
                     left)]
    (list left stage)))

(defn dependency-model [colls]
  "returns a list of order names orderd by referential integrity constraints. input is a sequence of context collection. output is a list of collection names"
  (let [x (map 
           (fn [coll]
             (list
              (:name coll) 
              (map #(.foreign-coll %) (filter #(instance? ReferrerIntegrityConstraint %) (-> coll :constraints deref vals))))) 
           colls)]
    (zipmap (map first x) (map last x))))

(defn dependency-model-ordered [colls]
  (let [names (apply concat (take-while not-empty (map last (rest (iterate dependency-order [(dependency-model colls)])))))
        coll-by-name (zipmap (map :name colls) colls)]
    (map #(get coll-by-name %) names)))

