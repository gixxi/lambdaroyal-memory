(ns lambdaroyal.memory.core.context
  (:require [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.core :refer [create-proxy]]
            [clojure.spec.alpha :as spec])
  (:import [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint ReferencedIntegrityConstraint AttributeIndex])
  (:refer-clojure :exclude [find])
  (:gen-class))

(spec/def ::referencing-coll keyword?)
(spec/def ::referencing-key keyword?)
(spec/def ::symbol #(instance? clojure.lang.Symbol %))
(spec/def ::name (spec/or :string string? :keyword keyword? :symbol ::symbol))
(spec/def ::referenced-constraint-name (spec/and map? (spec/keys :req-un [::referencing-coll ::referencing-key ::name])))
(spec/def ::referrer-constraint-name (spec/and map? (spec/keys :req-un [::foreign-coll ::foreing-key ::name])))

(defn referenced-constraint-name [referencing-coll referencing-key x]
  (cond
    (spec/valid? ::referenced-constraint-name x) x
    (spec/valid? ::name x) {:referencing-coll referencing-coll :referencing-key referencing-key :name x}
    :else (throw (IllegalArgumentException. (str "Cannot build referenced constraint name from " x)))))

(defn referrer-constraint-name [foreign-coll foreign-key x]
  (cond
    (spec/valid? ::referrer-constraint-name x) x
    (spec/valid? ::name x) {:foreign-coll foreign-coll :foreign-key foreign-key :name x}
    :else (throw (IllegalArgumentException. (str "Cannot build referrer constraint name from " x)))))

(defn- ric-factory 
  "use this function to create a set of indexes for a single referential integrity constraint [ric]. This function might be used in order to dynamically enrich a meta modell by a new ric. the ric (map) might contain :name :coll :foreign-coll :foreign-key :name"
  [ric]
  (let [{:keys [coll name foreign-coll foreign-key unique]} ric
        unique (or unique false)
        name (or name (gensym))
        constraint (create-referrer-integrity-constraint (referrer-constraint-name foreign-coll foreign-key name) foreign-coll foreign-key)]
    (list  
      [coll constraint]
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [coll (create-attribute-index (gensym) unique [(.foreign-key constraint)])]
      ;;add reverse constraint - RIC on the parent/referenced collection
      [(.foreign-coll constraint) (create-referenced-integrity-constraint (referenced-constraint-name coll (.foreign-key constraint) name) coll (.foreign-key constraint))])))

(defn add-attr-index
  "add an attribute index for attributes :attributes dynamically to a context [ctx] on behalf of collection :coll"
  [ctx coll attributes]
  (dosync
   (let [x [coll (create-attribute-index (gensym) false attributes)]]
     (let [[coll-name constraint] x
           ctx @ctx
           constraints (-> ctx coll-name :constraints)]
       (if (contains? @constraints (.name constraint)) (throw (IllegalArgumentException. (format "Constraint with name %s already given on collection %s" (.name constraints) coll))))
       (commute constraints assoc (.name constraint) constraint)
       ;;update and check indexes
       (if (contains? (.application constraint) :insert)
         (doseq [x (-> ctx coll-name :data deref)]
           (let [[k v] x]
             (.precommit constraint ctx coll-name :insert k v)
             (.postcommit constraint ctx coll-name :insert x))))))))

(defn remove-attr-index
  "removes all referential integrity constraints from the context [ctx] that refer the target collection [target] from the source collection [source]. The [foreign-key] within the source collection all the respective RICs need to match"
  [ctx coll attributes]
   (let [ctx @ctx
         source-coll (get ctx coll)] 
     (dosync
      ;;delete index backing the ric
      (doseq [constraint (filter #(= (.attributes %) attributes)
                                 (filter
                                  #(instance? AttributeIndex %) (map last (-> source-coll :constraints deref))))]
        (commute (:constraints source-coll) dissoc (.name constraint))))))

(defn add-ric 
  "add a referential integrity constraint dynamically to a context [ctx]"
  [ctx ric]
  (dosync
   (doseq [x (ric-factory ric)]
     (let [[coll-name constraint] x
           ctx @ctx
           constraints (-> ctx coll-name :constraints)]
       (if (contains? @constraints (.name constraint)) 
         (do
           (println :constraint constraint)
           (println :constaints-on-collection)
           (doseq [[k v] @constraints]
             (println k :constraint v))
           (throw (IllegalArgumentException. (format "Constraint with name %s already given on collection %s" (.name constraint) coll-name)))))
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
   (fn [acc [coll name unique constraint]]
     (conj 
      acc 
      [coll constraint]
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [coll (create-attribute-index (gensym) unique [(.foreign-key constraint)])]
      ;;add reverse constraint - RIC on the parent/referenced collection
      [(.foreign-coll constraint) (create-referenced-integrity-constraint (referenced-constraint-name coll (.foreign-key constraint) name) coll (.foreign-key constraint))]))
   []
   (map
    (fn [constraint]
      (let [{:keys [name coll foreign-coll foreign-key unique]} constraint
            name (or name (gensym))
            unique (or unique false)]
        [coll
         name
         unique
         (create-referrer-integrity-constraint (referrer-constraint-name foreign-coll foreign-key name) foreign-coll foreign-key)]))
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
        (alter (:constraints source-coll) dissoc (.name constraint)))))

(defn- delete-referenced-constraints
  "delete all referencing constraints from a collection [source] to a target collection [target]"
  [ctx source target]
  (let [ctx @ctx
        target-coll (get ctx target)]
      (doseq [constraint (filter #(= (.referencing-coll %) source)
                                 (filter
                                  #(instance? ReferencedIntegrityConstraint %) (map last (-> target-coll :constraints deref))))]
        (alter (:constraints target-coll) dissoc (.name constraint)))))

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

(defn- dependency-model 
  "returns a list of order names orderd by referential integrity constraints. input is a sequence of context collection. output is a list of collection names"
  [colls]
  (map 
   (fn [coll]
     (list
      (:name coll) 
      (set (map #(.foreign-coll %) (filter #(instance? ReferrerIntegrityConstraint %) (-> coll :constraints deref vals))))))
   colls))

(defn- partition-dependency-model 
  "returns [xs,xs'] from a NON-EMPTY dependency model, where xs is a set of colls that have only fulfilled dependencies or no dependencies at all and xs' still have dependencies"
  [dependency-model]
  (let [xs (set (map first (filter #(-> % last empty?) dependency-model)))]
    [xs (map
         (fn [[k v]]
           (list k (clojure.set/difference v xs)))
         (remove (fn [[k _]] (contains? xs k)) dependency-model))]))

(defn dependency-model-ordered [colls]
  (let [dependency-model (dependency-model colls)]
    (loop [[xs xs'] (partition-dependency-model dependency-model) res []]
      (cond
        (empty? xs') (apply conj res xs)
        (empty? xs) (throw (IllegalStateException. 
                            (str 
                             "failed to derive non-dependent partition from dependency model"
                             (apply str (interpose "," (map first dependency-model)))
                             \newline "problem in " (into [] xs'))))
        :else (recur (partition-dependency-model xs') (apply conj res xs))))))
