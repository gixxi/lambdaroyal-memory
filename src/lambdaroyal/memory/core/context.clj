(ns lambdaroyal.memory.core.context
  (require [lambdaroyal.memory.core.tx :refer :all])
  (:gen-class))

(defn- create-collection [collection]
  (let [fn-constraint-factory (fn [collection]
                                (if (:unique collection)
                                  {:unique-key (create-unique-key-constraint)}
                                  {}))
        fn-index-factory (fn [collection]
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
     :constraints (ref (merge (fn-index-factory collection) (fn-constraint-factory collection)))}))

(defn create-context [meta-model]
  (ref (zipmap (keys meta-model) (map #(create-collection %) 
                                      (map #(assoc %1 :name %2) (vals meta-model) (keys meta-model))))))


















