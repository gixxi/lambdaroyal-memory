(ns lambdaroyal.memory.core.tx
  (:import (lambdaroyal.memory.core ConstraintException)))

(defn create-tx [context]
  {:context context})

(def ^:const constraint-appl-domain 
  "donotes database actions suitable for certain types of domains"
  #{:insert :update :delete})

(defn create-constraint-exception [coll key value]
  (lambdaroyal.memory.core.ConstraintException. (format "Lambdaroyal-Memory Constraint Exception while handling key [%s] for collection [%s]: %s" coll key value)))

(defprotocol Constraint
  ""
  (precommit [this coll key value] "This function gets called before a key/value pair is inserted to/updated within a collection. Implementing this function if a necessary precondition needs to be checked before performing an costly update to the underlying datastructure holding the key/value pair. Implementations should raise ")
  (postcommit [this coll key value] "This function gets called after a key/value pair is inserted to/updated within a collection. Constraint like indexes implement this for the update of the very index after the update to the key/value pair data structure took place")
  (application [this] "returns a set of database actions this constraint type is relevant for"))

(defn UniqueKeyConstraint []
  (reify
    Constraint
    (precommit [this coll key value] (create-constraint-exception coll key value))
    (postcommit [this coll key value] nil)
    (application [this] #{:insert})))

(defn process-constraints [application f coll key value]
    (doseq [c (filter #(contains? (.application %) application) (-> coll :constraints deref vals))]
      (do
        (println :eval c f)
        (doto c (.precommit coll key value)))))

(defn insert [tx collection key value]
  {:pre [(contains? (-> tx :context deref) collection)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx collection)
        data (:data coll)]
    (do
      (process-constraints :insert precommit coll key value)
      (alter data assoc key (ref value))
      (process-constraints :insert postcommit coll key value))))

















