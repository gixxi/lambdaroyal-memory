(ns lambdaroyal.memory.core.context
  (require [lambdaroyal.memory.core.tx :refer :all])
  (require [lambdaroyal.memory.eviction.core :refer [create-proxy]])
  (import [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint])
  (:refer-clojure :exclude [find])
  (:gen-class))

(defn referential-integrity-constraint-factory [meta-model]
  (reduce
   (fn [acc [name constraint]]
     (conj 
      acc 
      [name constraint]
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [name (create-attribute-index (gensym) false [(.foreign-coll constraint)])]
      ;;add reverse constraint - RIC on the parent/referenced collection
      [(.foreign-coll constraint) (create-referenced-integrity-constraint (gensym) name (.foreign-key constraint))]))
   []
   (map
    (fn [constraint]
      (let [{:keys [name coll foreign-coll foreign-key]} constraint
            name (or name (gensym))]
        [coll
         (create-referrer-integrity-constraint name foreign-coll foreign-key)]))
    (reduce (fn [acc [k v]]
              (concat acc (map #(assoc % :coll k) v))) []
              (zipmap (keys meta-model) (map :foreign-key-constraints (vals meta-model)))))))

(defn- create-collection [collection meta-model referential-integrity-constraints]
  (let [fn-constraint-factory 
        (fn [collection]
          (if (:unique collection)
            {:unique-key (create-unique-key-constraint)}
            {}))
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

(defn create-context [meta-model]
  (let [rics (referential-integrity-constraint-factory meta-model)]
    (ref (zipmap (keys meta-model) 
                 (map 
                  #(create-collection % meta-model rics) 
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










