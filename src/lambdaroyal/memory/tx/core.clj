(ns lambdaroyal.memory.tx.core
  (:gen-class))

(defn create-tx [context]
  {:context context})

(defn insert [tx collection key value]
  {:pre [(contains? (:context tx) collection)]}
  (let [ctx (:context tx)
        coll (get ctx collection)
        data (-> coll deref :data)]
    (alter data assoc key (ref value))))
