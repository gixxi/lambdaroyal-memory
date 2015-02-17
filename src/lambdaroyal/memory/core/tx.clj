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
   [key (bigint 1)]))

(defn find-first 
  "returns the first raw key/value pair whose key is equal to the user key [key]"
  [coll key]
  (let [sub (subseq (-> coll :data deref) >= (create-unique-key key))]
    (first sub)))

(defn contains-key?
  "returns true iff the collection [coll] contains a tuple with the user key [key]"
  [coll key]
  (let [f (find-first coll key)]
    (and f (= key (-> f first first)))))

(defprotocol Constraint
  (precommit [this ctx coll key value] "This function gets called before a key/value pair is inserted to/updated within a collection. Implementing this function if a necessary precondition needs to be checked before performing an costly update to the underlying datastructure holding the key/value pair. Implementations should raise. Here ctx aims to handle constraint to need to check on other collections as well")
  (postcommit [this ctx coll coll-tuple] "This function gets called after a key/value pair is inserted to/updated within a collection. Constraint like indexes implement this for the update of the very index after the update to the key/value pair data structure took place")
  (application [this] "returns a set of database actions this constraint type is relevant for"))

(defprotocol Index
  ""
  (find [this start-test start-key stop-test stop-key]
    "takes all values from the collection using this index that fulfil (start-test start-key) until the collection is fully realized or (stop-test stop-key) is fulfilled. start-test as well as stop-test are of >,>=,<,<=. The returning sequence contains of items [[uk i] (ref v)], where uk is the user-key, i is the running index for the collection and (ref v) denotes a STM reference type instance to the value v"))

(defn- attribute-values 
  "returns a vector of all attribute values as per the attributes [attributes] for the value within coll-tuple <- [[k i] (ref value)]"
  [value attributes]
  (vec (map #(get value %) attributes)))

(deftype
    ^{:doc"A index implementation that is defined over a set of comparable attributes. The attributes are given as per the access keys that refer to the attributes to be indexed"}
  AttributeIndex [this unique attributes]
  Index
  (find [this start-test start-key stop-test stop-key]
    (let [this (.this this)]
      (map last (subseq (-> this :data deref) start-test (create-unique-key start-key) stop-test (create-unique-key stop-key)))))
  Constraint
  (application [this] #{:insert :delete})
  (precommit [this ctx coll key value]
    (let [this (.this this) 
          user-key (attribute-values value attributes)
          unique-key (create-unique-key this user-key)]
      (if (and unique (contains? this user-key))
        (throw (create-constraint-exception coll key (format "unique index constraint violated on index %s when precommit value %s" attributes value))))))
  (postcommit [this ctx coll coll-tuple]
    (let [this (.this this)
          user-key (attribute-values (-> coll-tuple last deref) attributes)
          unique-key (create-unique-key this user-key)]
      (commute (:data this) assoc unique-key coll-tuple))))

(defn create-attribute-index 
  "creates an attribute index for attributes a"
  [unique a]
  {:pre (sequential? a)}
  (AttributeIndex. 
   {:running (ref (bigint 0)) :data (ref (sorted-map))}
   unique
   a))

(defn create-unique-key-constraint []
  (reify
    Constraint
    (precommit [this ctx coll key value] 
      (if (contains-key? coll key)
        (throw (create-constraint-exception coll key "unique key constraint violated" ))))
    (postcommit [this ctx coll coll-tuple] nil)
    (application [this] #{:insert})))

(defn process-constraints [application f ctx coll & attr]
  (doseq [c (filter #(contains? (.application %) application) (-> coll :constraints deref vals))]
    (apply f c ctx coll attr)))

(defn insert [tx coll-name key value]
  "inserts a document [value] by key [key] into collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context])"
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)
        unique-key (create-unique-key coll key)
        coll-tuple [unique-key (ref value)]]
    (do
      (process-constraints :insert precommit ctx coll key value)
      (alter data assoc unique-key (last coll-tuple))
      (process-constraints :insert postcommit ctx coll coll-tuple))))

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
        (process-constraints :delete precommit ctx coll key (last x))
        (alter data dissoc (first x))
        (process-constraints :delete postcommit ctx coll x))
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

(defn select-first
  "returns the first user key/value pair of the collection [coll-name] that matches the key [key] or nil"
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (if-let [f (find-first (get (-> tx :context deref) coll-name) key)]
    [(-> f first first) (last f)]))

(defn select
  "test(s) one of <, <=, > or
>=. Returns a seq of those entries [user key, value] with keys ek for
which (test (.. sc comparator (compare ek key)) 0) is true"
  ([tx coll-name start-test start-key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test (create-unique-key start-key))]
    (map (fn [[[uk i] v]] [uk v]) sub)))

  ([tx coll-name start-test start-key stop-test stop-key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test (create-unique-key start-key) stop-test (create-unique-key stop-key))]
    (map (fn [[[uk i] v]] [uk v]) sub))))
















