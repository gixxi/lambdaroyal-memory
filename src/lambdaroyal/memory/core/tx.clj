(ns lambdaroyal.memory.core.tx
  (:require (lambdaroyal.memory.core))
  (:require [lambdaroyal.memory.eviction.core :as evict])
  (:import [lambdaroyal.memory.core ConstraintException])
  (:refer-clojure :exclude [update find])
  (:gen-class))

(def ^:const debug (atom false))

(defn create-tx 
  "creates a transaction upon user-scope function like select, insert, alter-document, delete can be executed. Iff an eviction channel is assigned to a collection then this channel needs to be started otherwise a "
  [ctx & opts]
  (let [opts (apply hash-map opts)
        {:keys [force]} opts]
    (do
      (if-not force
        (doseq [coll (vals @ctx)]
          (if-let [eviction-proxy (:evictor coll)]
            (if-not 
                (.started? eviction-proxy)
              (throw (IllegalStateException. (format "eviction channel for collection %s is not yet started." (:coll-name coll))))))))
      {:context ctx})))

(def ^:const constraint-appl-domain 
  "donotes database actions suitable for certain types of domains"
  #{:insert :update :delete})

(defn create-constraint-exception [coll key msg]
  (lambdaroyal.memory.core.ConstraintException. (format "Lambdaroyal-Memory Constraint Exception while handling key [%s] for collection [%s]: %s" key (:name coll) msg)))

(defn create-no-applicable-index-exception [coll key]
  (lambdaroyal.memory.core.ConstraintException. (format "Lambdaroyal-Memory No applicable index defined for key %s on collection [%s]" key (:name coll))))

(defn- evictor-watch 
  "returns an stm ref watch that calls the evictor when inserting the stm ref, when updating the respetive value and when deleting the stm ref"
  [coll]
  (fn [watch ref old new]
    (if-let [evictor (-> coll :evictor)]
      (let [_ (comment (println :evict :ref ref :meta (meta ref) :old old :new new))
            coll-name (-> ref meta :coll-name)
            deleted (-> ref meta :deleted)
            unique-key (-> ref meta :unique-key)]
        (cond
         (nil? old) (evict/insert evictor coll-name unique-key new)
         (-> ref meta :deleted) (evict/delete evictor coll-name unique-key)
         :else (evict/update evictor coll-name unique-key old new))))))

(defn- value-wrapper
  "takes a value [user-value] to be stored into the database and returns a respective STM ref with meta-data attached used for reverse index key handling. this map denotes key/value pairs, where key is the name of a index refering the inserted user-value as well as value denotes the key within this very index"
  [coll unique-key user-value]
  (let [x (ref (if (:evictor coll) nil user-value) :meta {:coll-name (:name coll) :unique-key unique-key :idx-keys (ref {})})]
    (do 
      (if-let [eviction-proxy (:evictor coll)]
        (do
          (add-watch x :evictor (evictor-watch coll))
          (ref-set x user-value)))
      x)))

(defn- get-idx-keys
  "takes a value-wrapper into account, that is the wrapper around the user value that is inserted into the database and returns the STM ref to the reverse lookup map from the index name to the key that refers this value-wrapper within this very index"
  [value-wrapper]
  (-> value-wrapper meta :idx-keys))

(defn- is-unique-key? [key]
  (let [m (meta key)]
    (or (and m
             (-> m :unique-key true?)) false)))

(defn create-unique-key 
  "creates a unique key [key running] from a user space key using the running bigint index"
  ([coll key]
   (if (is-unique-key? key)
     key
     (with-meta [key (alter (:running coll) inc)] {:unique-key true})))
  ([key]
   (if (is-unique-key? key)
     key
     [key (bigint 1)])))

(defn- stage-next-unique-key
  "returns the next key that would be used for the collection [coll] and the given user key [key]. this can be used to return bounded subsets that match only those 
  documents associated with the user key [key]"
  [coll key]
  (with-meta [key (-> coll :running deref inc)] {:unique-key true}))

(defn find-first "returns the first collection tuple whose key is equal to the user key [key]. This is not a userscope method."
    [coll key]
    (if-let [v (get (-> coll :data deref) key)]
      [key v]))

(defn contains-key? "returns true iff the collection [coll] contains a tuple whose key is equal to the user key [key]."
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
  (find-without-stop [this start-test start-key]
    "takes all values from the collection using this index that fulfil (start-test start-key) until the collection is fully realized. start-test is of >,>=,<,<=. The returning sequence contains of items [[uk i] (ref v)], where uk is the user-key, i is the running index for the collection and (ref v) denotes a STM reference type instance to the value v")
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
      (let [this (.this this)
            start-key (create-unique-key start-key)
            stop-key (create-unique-key stop-key)
            data (-> this :data deref)]
        (map last (subseq (-> this :data deref) start-test start-key stop-test stop-key))))
    (find-without-stop [this start-test start-key]
      (let [this (.this this)
            start-key (create-unique-key start-key)]
        (map last (subseq (-> this :data deref) start-test start-key))))
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
        (let [user-key (attribute-values value attributes)
              unique-key (create-unique-key (.this this) user-key)]
          (if unique 
            (if-let [match (first (.find-without-stop this >= user-key))]
              (if (= user-key (attribute-values (-> match last deref) attributes))
                (throw (create-constraint-exception coll key (format "unique index constraint violated on index %s when precommit value %s" attributes value)))))))))
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
               idx-keys (-> coll-tuple get-idx-keys)]
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

(defn select-from-coll 
  ([coll attributes start-test start-key stop-test stop-key]
   (let [indexes (applicable-indexes coll attributes)]
     (if-let [index (first indexes)]
       (.find index start-test start-key stop-test stop-key)
       (throw (create-no-applicable-index-exception coll attributes)))))
  ([coll attributes start-test start-key]
   (let [indexes (applicable-indexes coll attributes)]
     (if-let [index (first indexes)]
       (.find-without-stop index start-test start-key)
       (throw (create-no-applicable-index-exception coll attributes))))))


(deftype
    ^{:doc "The child (foreign key) part of the referential integrity constraint that checks during insert and alterations of documents refering a referenced/parent document"}
    ReferrerIntegrityConstraint [this name foreign-coll foreign-key]
    Constraint
    (application [this] #{:insert :update})
    (precommit [this ctx coll application key value]
      {:pre [(contains? ctx foreign-coll)]}
      (let [foreign-coll (get ctx foreign-coll)]
        ;;check whether the user-value has a non-nil foreign key,
        ;;otherwise we don't have to check at all
        (if-let [foreign-key (get value foreign-key)]
          (if-not (contains-key? foreign-coll foreign-key)
            (throw (create-constraint-exception coll key (format "referrer integrity constraint violated. no document with key %s within collection %s" foreign-key (.foreign-coll this))))))))
    (postcommit [this ctx coll application coll-tuple] nil))

(defn create-referrer-integrity-constraint 
  [name foreign-coll foreign-key]
  (ReferrerIntegrityConstraint. 
   {}
   name
   foreign-coll
   foreign-key))

(deftype
    ^{:doc "The referenced/parent (primary key) part of the referential integrity constraint that checks during deleting of a documents whether it is referenced by another document"}
    ReferencedIntegrityConstraint [this name referencing-coll referencing-key]
    Constraint
    (application [this] #{:delete})
    (precommit [this ctx coll application key value]
      {:pre [(contains? ctx referencing-coll)]}
      (let [;;select the referencee using the automatically generated index on the referencing-key
            match (first (select-from-coll (get ctx referencing-coll) [referencing-key] >= [key]))]
        (if (and match (= (-> match meta :unique-key)))
          (throw (create-constraint-exception coll key (format "referenced integrity constraint violated. a document with key %s within collection %s references this document" referencing-coll referencing-key))))))
    (postcommit [this ctx coll application coll-tuple] nil))

(defn create-referenced-integrity-constraint 
  [name referencing-coll referencing-key]
  (ReferencedIntegrityConstraint. 
   {}
   name
   referencing-coll
   referencing-key))

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

(defn user-scope-tuple 
  "within a collection a key-value-pair consists of k -> r, where k is [uk i], r is a STM reference to document v, uk
  is the key the user used to store document v. i is a running index that allows
  the uk being non-unique with respect to the collection. since v is wrapped by an STM reference we provide back the _raw_ value v in order to prevent the user from altering the value using STM mechanism directly, since this would bypass the secondary indexes and make them invalid."
  [[k r]]
  [k @r])

(defn user-scope-key
  "takes a user-scope-tuple into account and returns the user scope key that was provided when storing this document"
  [user-scope-tuple]
  (-> user-scope-tuple first))

(defn insert [tx coll-name key value]
  "inserts a document [value] by key [key] into collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context])"
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)
        coll-tuple [key (value-wrapper coll key value)]]
    (do
      (process-constraints :insert precommit ctx coll key value)
      (alter data assoc key (last coll-tuple))
      (process-constraints :insert postcommit ctx coll coll-tuple)
      (user-scope-tuple coll-tuple))))


(defn- alter-index
  [idx coll-tuple old-user-value new-user-value]
  (let [old-attribute-values (attribute-values old-user-value (.attributes idx))
        new-attribute-values (attribute-values new-user-value (.attributes idx))]
    (if 
        (not= 0 (compare old-attribute-values new-attribute-values))
      (let [idx-keys (-> coll-tuple last get-idx-keys)]
        (if-let [idx-key (get @idx-keys (.name idx))]
          (let [new-unique-index-key (create-unique-key (.this idx) new-attribute-values)]
            (do
              ;;remove old index tuple
              (alter (-> idx .this :data) dissoc idx-key)
              ;;add new index tuple
              (alter (-> idx .this :data) assoc new-unique-index-key coll-tuple)
              ;;alter reverse lookup
              (alter idx-keys assoc (.name idx) new-unique-index-key)
              (comment (print (.name idx) :old old-user-value :new new-user-value :old-a old-attribute-values :new-a new-attribute-values :idx-keys idx-keys))
              ))
          (throw (RuntimeException. (format "FATAL RUNTIME EXCEPTION: index %s is inconsistent, failed to remove key %s from value-wrapper %s. Failed to reverse lookup index key." name coll-tuple))))))))

(defn alter-document
  "alters a document given by [user-scope-tuple] within the collection denoted by [coll-name] by applying the function [fn] with the parameters [args] to it. An user-scope-tuple can be obtained using find-first, find and select. returns the new user value"
  [tx coll-name user-scope-tuple fn & args]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        coll-tuple (find-first coll (first user-scope-tuple))
        _ (if (nil? coll-tuple) 
            (throw (create-constraint-exception coll key "cannot alter document since document is not present in the collection" )))
        old-user-value (last user-scope-tuple)
        constraints (map last (-> coll :constraints deref))
        idxs (filter
              #(satisfies? Index %) constraints)
        rics (filter
              #(instance? ReferrerIntegrityConstraint %) constraints)
        new-user-value (apply alter (last coll-tuple) fn args)]
    (do
      ;;check all referential integrity constraints on the referrer site of the coin
      (doseq [_ rics]
        (.precommit _ ctx coll :update (first user-scope-tuple) new-user-value))
      ;;alter all indexes to consider the document change
      (doseq [idx idxs]
        (alter-index idx coll-tuple old-user-value new-user-value))
      new-user-value)))

(defn delete 
  "deletes a document by key [key] from collection with name [coll-name] using the transaction [tx]. the transaction can be created from context using (create-tx [context]. returns number of removed items."
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [ctx (-> tx :context deref)
        coll (get ctx coll-name)
        data (:data coll)]
    (if-let [x (get @data key)]
      (do
        (process-constraints :delete precommit ctx coll key @x)
        (alter data dissoc key)
        (process-constraints :delete postcommit ctx coll x)
        (alter-meta! x assoc :deleted true)
        ;;set again to notify eviction watches
        (ref-set x @x)
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

(defn select-first
  "returns the first key/value pair of the collection [coll-name] that matches the key [key] or nil"
  [tx coll-name key]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (if-let [f (find-first (get (-> tx :context deref) coll-name) key)]
    (user-scope-tuple f)))

(defn select
  "test(s) one of <, <=, > or
  >=. Returns a seq of those entries [key, value] with keys ek for
  which (test (.. sc comparator (compare ek key)) 0) is true. iff just [tx] and coll-name are given, then we return all tupels."
  ([tx coll-name]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [all (-> (get  (-> tx :context deref) coll-name) :data deref)]
     (map user-scope-tuple all)))
  ([tx coll-name start-test start-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test start-key)]
     (map user-scope-tuple sub)))

  ([tx coll-name start-test start-key stop-test stop-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [sub (subseq (-> (get  (-> tx :context deref) coll-name) :data deref) start-test start-key stop-test stop-key)]
     (map user-scope-tuple sub)))

  ([tx coll-name attributes start-test start-key stop-test stop-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [coll (get (-> tx :context deref) coll-name)]
     (map user-scope-tuple (select-from-coll coll attributes start-test start-key stop-test stop-key))))
  ([tx coll-name attributes start-test start-key]
   {:pre [(contains? (-> tx :context deref) coll-name)]}
   (let [coll (get (-> tx :context deref) coll-name)]
     (map user-scope-tuple (select-from-coll coll attributes start-test start-key)))))

(defn tree-referencees
  "takes a document [user-scope-tuple] from the collection with name [coll-name] and gives back a hash-map denoting all foreign-key referenced documents. The key in hash-map is [coll-name user-scope-key]."
  [tx coll-name user-scope-tuple & opts]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (let [opts (apply hash-map opts)]
    (loop [todo #{[coll-name user-scope-tuple]} done (or (:cache opts) {})]
      (if (empty? todo)
        done
        ;;else
        (let [
              ctx (-> tx :context deref)
              coll (get ctx coll-name)
              next (first todo)
              constraints (map last (-> coll :constraints deref))
              idxs (filter
                    #(satisfies? Index %) constraints)
              rics (filter
                    #(instance? ReferrerIntegrityConstraint %) constraints)
              ;;filter out all rics that denote an entity already within 'done
              rics (remove (fn [ric]
                             (let [foreign-key (get (-> next last last) (.foreign-key ric))]
                               (or
                                (nil? foreign-key)
                                (contains? done [(.foreign-coll ric) foreign-key]))))
                           rics)
              done (reduce
                    (fn [acc ric]
                      (let [foreign-key (get (-> next last last) (.foreign-key ric))]
                        (assoc acc [(.foreign-coll ric) foreign-key]
                               [(.foreign-coll ric) (select-first tx (.foreign-coll ric) foreign-key)])))
                    done
                    rics)]
          (recur (rest todo) done))))))

(defn- replace-in-tree [tx coll-name user-scope-tuple referencees & opts]
  (let [referencees (apply tree-referencees tx coll-name user-scope-tuple opts)
        ctx (-> tx :context deref)
        coll (get ctx coll-name)
        constraints (map last (-> coll :constraints deref))
        idxs (filter
              #(satisfies? Index %) constraints)
        rics (filter
              #(instance? ReferrerIntegrityConstraint %) constraints)
        ;;filter out all rics that are not used in user-scope-tuple
        rics (remove (fn [ric]
                       (nil? (get (last user-scope-tuple) (.foreign-key ric))))
                     rics)
        ;;get a map of key->referencee
        merge-map (reduce
                   (fn [acc ric]
                     (assoc acc (.foreign-coll ric)
                            (apply replace-in-tree tx (.foreign-coll ric) (last (get referencees [(.foreign-coll ric) (get (last user-scope-tuple) (.foreign-key ric))])) referencees opts)
                            ))
                   {} rics)]
    [(first user-scope-tuple) (assoc 
                                  (merge (last user-scope-tuple) merge-map)
                                :coll coll-name)]))

(defn tree
  "takes a document [user-scope-tuple] from the collection with name [coll-name] and gives back derived document where all foreign keys are replaced by their respective documents."
  [tx coll-name user-scope-tuple & opts]
  {:pre [(contains? (-> tx :context deref) coll-name)]}
  (replace-in-tree tx coll-name user-scope-tuple tree-referencees :cache {}))
