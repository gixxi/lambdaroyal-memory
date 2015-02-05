(ns lambdaroyal.memory.context.factory
    (:gen-class))

(defn- create-collection [collection]
  (ref {:data (ref (sorted-map))
        :constraints (ref {})}))

(defn create-context [meta-model]
  (zipmap (keys meta-model) (map #(create-collection %) (vals meta-model))))
