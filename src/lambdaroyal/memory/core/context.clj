(ns lambdaroyal.memory.core.context
  (require [lambdaroyal.memory.core.tx :refer :all])
  (:gen-class))

(defn- create-collection [collection]
  (let [fn-constraint-factory (fn [collection]
                                (if (:unique collection)
                                  {:unique-key (create-unique-key-constraint)}
                                  {}))]
    
    {:running (ref (bigint 0))
     :name (:name collection)
     :data (ref (sorted-map))
     :constraints (ref (fn-constraint-factory collection))}))

(defn create-context [meta-model]
  (ref (zipmap (keys meta-model) (map #(create-collection %) 
                                      (map #(assoc %1 :name %2) (vals meta-model) (keys meta-model))))))


















