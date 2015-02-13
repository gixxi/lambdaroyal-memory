(ns lambdaroyal.memory.core.tx
  (:require (lambdaroyal.memory.core))
  (:import (lambdaroyal.memory.core ConstraintException))
  (:gen-class))

(defn create-tx [context]
  {:context context})

(def ^:const constraint-appl-domain 
  "donotes database actions suitable for certain types of domains"
  #{:insert :update :delete})

(defn create-constraint-exception [coll key msg]
  (lambdaroyal.memory.core.ConstraintException. (format "Lambdaroyal-Memory Constraint Exception while handling key [%s] for collection [%s]: %s" key (:name coll) msg)))

(defn create-unique-key 
  "creates a unique key [key running] from a user space key using the running bigint index"
  ([coll key]
   [key (alter (:running coll) inc)])
  ([key]
   [key (bigint 0)]))

(defn find-first 
  "returns true iff the collection [coll] contains a tuple with the user key [key]"
  [coll key]
  (let [sub (subseq (-> coll :data deref) >= (create-unique-key key))]
    (first sub)))

(defn contains-key?
  "returns true iff the collection [coll] contains a tuple with the user key [key]"
  [coll key]
  (let [f (find-first coll key)]
    (and f (= key (-> f first first)))))

(defprotocol Constraint
  ""
  (precommit [this coll key value] "This function gets called before a key/value pair is inserted to/updated within a collection. Implementing this function if a necessary precondition needs to be checked before performing an costly update to the underlying datastructure holding the key/value pair. Implementations should raise ")
  (postcommit [this coll key value] "This function gets called after a key/value pair is inserted to/updated within a collection. Constraint like indexes implement this for the update of the very index after the update to the key/value pair data structure took place")
  (application [this] "returns a set of database actions this constraint type is relevant for"))

(defn create-unique-key-constraint []
  (reify
    Constraint
    (precommit [this coll key value] 
      (if (contains-key? coll key)
        (throw (create-constraint-exception coll key "unique key constraint violated" ))))
    (postcommit [this coll key value] nil)
    (application [this] #{:insert})))

(defn process-constraints [application f coll key value]
  (doseq [c (filter #(contains? (.application %) application) (-> coll :constraints deref vals))]
    (f c coll key value)))

(defn insert [tx coll-name key value]
  "inserts a document [value] by key [key] into collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context])"
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)
        unique-key (create-unique-key coll key)]
    (do
      (process-constraints :insert precommit coll key value)
      (alter data assoc unique-key (ref value))
      (process-constraints :insert postcommit coll key value))))

(defn delete
  "deletes a document by key [key] from collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context]. returns number of removed items)"
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)
        sub (subseq @data >= (create-unique-key key))]
    (let [tuples-to-delete (take-while #(= key (-> % first first)) sub)]
      (doseq [x tuples-to-delete]
        (process-constraints :delete precommit coll key (last x))
        (alter data dissoc (first x))
        (process-constraints :delete postcommit coll key (last x)))
      (count tuples-to-delete))))

(defn coll-empty? 
  "returns true iff the collection with name [coll-name] is empty"
  [tx coll-name]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (-> (get (-> tx :context deref) coll-name) :data deref empty?))

(defn coll-count
  "returns the number of elements in a collection with name [coll-name]"
  [tx coll-name]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (-> (get (-> tx :context deref) coll-name) :data deref count))















