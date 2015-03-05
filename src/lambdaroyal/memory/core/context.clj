(ns lambdaroyal.memory.core.context
  (require [lambdaroyal.memory.core.tx :refer :all])
  (:gen-class))

(defn referential-integrity-constraint-factory [meta-model]
  (reduce
   (fn [acc [name constraint]]
     (conj 
      acc 
      [name constraint]
      ;;add reverse constraint - RIC on the parent/referenced collection
 
      ;;add additional index that backs looking up referrers during deleting parent documents 
      [name (create-attribute-index (gensym) false [(.foreign-coll constraint)])]))
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
                   (create-attribute-index name unique (map first attributes))))))
           {}
           (:indexes collection)))]
    
    {:running (ref (bigint 0))
     :name (:name collection)
     :data (ref (sorted-map))
     :constraints (ref 
                   (merge 
                    (fn-index-factory collection) 
                    (fn-constraint-factory collection) 
                    (reduce 
                     (fn [acc [coll constraint]]
                       (if (= (:name collection) coll)
                         (assoc acc (.name constraint) constraint)))
                     {}
                     referential-integrity-constraints)))}))

(defn create-context [meta-model]
  (let [rics (referential-integrity-constraint-factory meta-model)]
    (ref (zipmap (keys meta-model) 
                 (map 
                  #(create-collection % meta-model rics) 
                  (map #(assoc %1 :name %2) (vals meta-model) (keys meta-model)))))))


















