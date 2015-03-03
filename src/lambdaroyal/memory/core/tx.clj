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

(defn create-no-applicable-index-exception [coll key]
  (lambdaroyal.memory.core.ConstraintException. (format "Lambdaroyal-Memory No applicable index defined for key %s on collection [%s]" key (:name coll))))

(defn- value-wrapper
  "takes a value [user-value] to be stored into the database and returns a respective STM ref with meta-data attached used for reverse index key handling. this map denotes key/value pairs, where key is the name of a index refering the inserted user-value as well as value denotes the key within this very index"
  [user-value]
  (ref user-value :meta {:idx-keys (ref {})}))

(defn- get-idx-keys
  "takes a value-wrapper into account, that is the wrapper around the user value that is inserted into the database and returns the STM ref to the reverse lookup map from the index name to the key that refers this value-wrapper within this very index"
  [value-wrapper]
  (-> value-wrapper meta :idx-keys))

(defn create-unique-key 
  "creates a unique key [key running] from a user space key using the running bigint index"
  ([coll key]
   (with-meta [key (alter (:running coll) inc)] {:unique-key true}))
  ([key]
   [key (bigint 1)]))

(defn- is-unique-key? [key]
  (let [m (meta key)]
    (or (and m
             (-> m :unique-key true?)) false)))

(defmulti find-first "returns the first collection tuple whose key is equal to the user key [key]. This multimethod supports both user keys as well as unique keys as inputs. this is not a userscope method." (fn [coll key] (is-unique-key? key)))

(defmethod find-first false
  [coll key]
  (let [sub (subseq (-> coll :data deref) >= (create-unique-key key))]
    (first sub)))

(defmethod find-first true
  [coll key]
  (if-let [v (get (-> coll :data deref) key)]
    [key v]))

(defmulti contains-key? "returns true iff the collection [coll] contains a tuple whose key is equal to the user key [key]. This multimethod supports both user keys as well as unique keys as inputs." (fn [coll key] (is-unique-key? key)))

(defmethod contains-key? false
  [coll key]
  (let [f (find-first coll key)]
    (and f (= key (-> f first first)))))

(defmethod contains-key? true
  [coll key]
  (contains? (-> coll :data deref) key))


(defprotocol Constraint
  (precommit [this ctx coll application key value] "This function gets called before a key/value pair is inserted to/updated within a collection. Implementing this function if a necessary precondition needs to be checked before performing an costly update to the underlying datastructure holding the key/value pair. Implementations should raise. Here ctx aims to handle constraint to need to check on other collections as well")
  (postcommit [this ctx coll application coll-tuple] "This function gets called after a key/value pair is inserted to/updated within a collection. Constraint like indexes implement this for the update of the very index after the update to the key/value pair data structure took place")
  (application [this] "returns a set of database actions this constraint type is relevant for"))

(defprotocol Index
  ""
  (find [this start-test start-key stop-test stop-key]
    "takes all values from the collection using this index that fulfil (start-test start-key) until the collection is fully realized or (stop-test stop-key) is fulfilled. start-test as well as stop-test are of >,>=,<,<=. The returning sequence contains of items [[uk i] (ref v)], where uk is the user-key, i is the running index for the collection and (ref v) denotes a STM reference type instance to the value v")
  (applicable? [this key]
    "return true iff this index can be used to find values as per the given key.")
  (rating [this key]
    "returns a natural number denoting an order by which two indexes can be compared in order to use one for a finding a certain key. the index with the lower rating result wins"))

(defn- attribute-values 
  "returns a vector of all attribute values as per the attributes [attributes] for the value within coll-tuple <- [[k i] (ref value)]"
  [value attributes]
  (vec (map #(get value %) attributes)))

(deftype
    ^{:doc"A index implementation that is defined over a set of comparable attributes. The attributes are given as per the access keys that refer to the attributes to be indexed"}
    AttributeIndex [this name unique attributes]
    Index
    (find [this start-test start-key stop-test stop-key]
      (let [this (.this this)]
        (map last (subseq (-> this :data deref) start-test (create-unique-key start-key) stop-test (create-unique-key stop-key)))))
    (applicable? [this key]
      (and
       (sequential? key)
       (>= (-> this .attributes count) (count key))
       (not-empty (take-while true?
                              (map (fn [[a b]] (= a b))
                                   (map list (.attributes this) key))))))
    (rating [this key]
      (count attributes))
    Constraint
    (application [this] #{:insert :delete})
    (precommit [this ctx coll application key value]
      (if (= :insert application)
        (let [this (.this this) 
              user-key (attribute-values value attributes)
              unique-key (create-unique-key this user-key)]
          (if (and unique (contains? this user-key))
            (throw (create-constraint-exception coll key (format "unique index constraint violated on index %s when precommit value %s" attributes value)))))))
    (postcommit [this ctx coll application coll-tuple]
      (cond
       (= :insert application)
       (let [this (.this this)
             user-value (-> coll-tuple last deref)
             user-key (attribute-values user-value attributes)
             unique-key (create-unique-key this user-key)]
         (alter (-> coll-tuple last get-idx-keys) assoc name unique-key)
         (alter (:data this) assoc unique-key coll-tuple))
         (= :delete application)
         (if coll-tuple
           (let [this (.this this) 
                 idx-keys (-> coll-tuple last get-idx-keys)]
             (if-let [idx-key (get @idx-keys name)]
               (alter (:data this) dissoc idx-key)
               (throw (RuntimeException. (format "FATAL RUNTIME EXCEPTION: index %s is inconsistent, failed to remove key %s from value-wrapper %s. Failed to reverse lookup index key." name coll-tuple)))))))))

(defn create-attribute-index 
  "creates an attribute index for attributes a"
  [name unique a]
  {:pre (sequential? a)}
  (AttributeIndex. 
   {:running (ref (bigint 0)) :data (ref (sorted-map))}
   name
   unique
   a))

(defn applicable-indexes [coll key]
  (sort-by #(.rating % key)
           (filter 
            #(.applicable? % key)
            (filter
             #(satisfies? Index %) (map last (-> coll :constraints deref))))))

(defn create-unique-key-constraint []
  (reify
    Constraint
    (precommit [this ctx coll application key value] 
      (if (contains-key? coll key)
        (throw (create-constraint-exception coll key "unique key constraint violated" ))))
    (postcommit [this ctx coll application coll-tuple] nil)
    (application [this] #{:insert})))

(defn process-constraints [application f ctx coll & attr]
  (doseq [c (filter #(contains? (.application %) application) (-> coll :constraints deref vals))]
    (apply f c ctx coll application attr)))

(defn insert [tx coll-name key value]
  "inserts a document [value] by key [key] into collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context])"
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)
        unique-key (create-unique-key coll key)
        coll-tuple [unique-key (value-wrapper value)]]
    (do
      (process-constraints :insert precommit ctx coll key value)
      (alter data assoc unique-key (last coll-tuple))
      (process-constraints :insert postcommit ctx coll coll-tuple))))

(defn alter-document
  "alters a document given by [user-scope-tuple] within the collection denoted by [coll-name] by applying the function [fn] with the parameters [args] to it. An user-scope-tuple can be obtained using find-first, find and select"
 [tx coll-name user-scope-tuple fn & args]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        ref (find-first coll (first user-scope-tuple))
        _ (if (ref? val) 
            (throw (create-constraint-exception coll key "cannot alter document since document is not present in the collection" )))))
        
        idxs (filter
              #(satisfies? Index %) (map last (-> coll :constraints deref)))
        current-user-value (last user-scope-tuple)
        current-idxs-attr (map #(attribute-values current-user-value (.attributes %) idxs))
        new-user-value (alter ref fn args)
        new-idxs-attr (map #(attribute-values current-user-value (.attributes %) idxs))]
    (do
      (doall
       (map (fn [idx old new]
              (if
                  (not= old new))))))))

(defmulti delete 
  "deletes a document by key [key] from collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context]. returns number of removed items. works both with user keys as well as unique keys)"
  (fn [tx coll-name key] (is-unique-key? key)))

(defmethod delete false
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

(defn- user-key [k]
  (first k))

(defmethod delete true
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)]
    (if-let [x (get @data key)]
      (do
        (process-constraints :delete precommit ctx coll (user-key key) x)
        (alter data dissoc key)
        (process-constraints :delete postcommit ctx coll [key x])
        1) 0)))

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

(defn user-scope-tuple 
  "within a collection a key-value-pair consists of k -> r, where k is [uk i], r is a STM reference to document v, uk
  is the key the user used to store document v. i is a running index that allows
  the uk being non-unique with respect to the collection. since v is wrapped by an STM reference we provide back the _raw_ value v in order to prevent the user from altering the value using STM mechanism directly, since this would bypass the secondary indexes and make them invalid."
  [[k r]]
  [k @r])

(defn select-first
  "returns the first key/value pair of the collection [coll-name] that matches the key [key] or nil"
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (if-let [f (find-first (get (-> tx :context deref) coll-name) key)]
    (user-scope-tuple f)))

(defn select
  "test(s) one of <, <=, > or
  >=. Returns a seq of those entries [key, value] with keys ek for
  which (test (.. sc comparator (compare ek key)) 0) is true"
  ([tx coll-name start-test start-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test (create-unique-key start-key))]
     (map user-scope-tuple sub)))

  ([tx coll-name start-test start-key stop-test stop-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test (create-unique-key start-key) stop-test (create-unique-key stop-key))]
     (map user-scope-tuple sub)))

  ([tx coll-name attributes start-test start-key stop-test stop-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [coll (get (-> tx :context deref) coll-name)
         indexes (applicable-indexes coll attributes)]
     (if-let [index (first indexes)]
       (.find index start-test start-key stop-test stop-key)
       (throw (create-no-applicable-index-exception coll attributes))))))


















