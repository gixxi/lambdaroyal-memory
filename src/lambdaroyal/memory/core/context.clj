(ns lambdaroyal.memory.core.context
  (import [lambdaroyal.memory.core.tx UniqueKeyConstraint])
  (:gen-class))

(defn- create-collection [collection]
  (let [fn-constraint-factory (fn [collection]
                                (if (:unique collection)
                                  {:unique-key (UniqueKeyConstraint.)}
                                  {}))]
    
    {:data (ref (sorted-map))
     :constraints (ref (fn-constraint-factory collection))}))

(defn create-context [meta-model]
  (ref (zipmap (keys meta-model) (map #(create-collection %) (vals meta-model)))))








