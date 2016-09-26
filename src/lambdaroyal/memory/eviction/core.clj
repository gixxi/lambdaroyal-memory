(ns ^{:doc "an eviction channel listenes for successful database inserts/updates and deletes and performs some kind of I/O to them. For instance a concrete implementation could write all successful changes to the in-memory database to a external database to create a persistent version that can be imported during application ramp-up. Eviction channels can be configured to follow certain timing constraints like immediate post transaction eviction or cron like eviction, that is forwarding all the changes every x seconds. Invariants are: (1) Eviction is asynchronous to to the actual transaction, (2) Eviction might fail but to not cause the transaction to fail."} 
  lambdaroyal.memory.eviction.core
  (:refer-clojure :exclude [update])
  (:gen-class))

(defprotocol EvictionChannel
  (start [this ctx colls] "starts the eviction channel by reading in the persistent data into the in-memory db denoted by [ctx]. This function assumes that is eviction channel instance is handling all the collection [colls]. returns a future that can be joined to wait for the database to got warmed up.")
  (started? [this])
  (stop [this] "closes the channel. Further evictions must throw an exception.")
  (stopped? [this] "returns true iff the channel was closed.")
  (insert [this coll-name unique-key user-value] "called when a new value gets inserted into the database.")
  (update [this coll-name unique-key old-user-value new-user-value] "called when an update took place")
  (delete [this coll-name unique-key] "called when a delete took place")
  (delete-coll [this coll-name] "call this to delete the respective collection/db at all"))

(defrecord EvictionChannelProxy [queue delay stopped eviction-channel]
  EvictionChannel
  (start [this ctx colls] (.start (.eviction-channel this) ctx colls))
  (started? [this] (.started? (.eviction-channel this)))
  (stop [this] (swap! (.stopped this) not))
  (stopped? [this] (true? @(.stopped this)))
  (insert [this coll-name unique-key user-value]
    (if (-> this .eviction-channel .started?)
      (.add (.queue this) [:insert eviction-channel coll-name unique-key user-value])))
  (update [this coll-name unique-key old-user-value new-user-value]
    (if (-> this .eviction-channel .started?)
      (.add (.queue this) [:update eviction-channel coll-name unique-key old-user-value new-user-value])))
  (delete [this coll-name unique-key]
    (if (-> this .eviction-channel .started?)
    (.add (.queue this) [:delete eviction-channel coll-name unique-key])))
  (delete-coll [this coll-name]
    (if (-> this .eviction-channel .started?)
      (.delete-coll (-> this .eviction-channel) coll-name))))

(defn create-proxy [eviction-channel delay]
  (let [proxy
        (EvictionChannelProxy. 
         (java.util.concurrent.ConcurrentLinkedQueue.) 
         ;; at least 100 ms pause whenever the queue gets empty
         (or delay 100) 
         (atom false) 
         eviction-channel)
        consumer
        (future
          (do
            (while (or 
                    (-> proxy stopped? not)
                    (not (.isEmpty (.queue proxy))))
              (if-let [i (.poll (.queue proxy))]
                (let [[fn & args] i]
                  (cond (= :insert fn)
                        (apply insert args)
                        (= :update fn)
                        (apply update args)
                        (= :delete fn)
                        (apply delete args)))
                (Thread/sleep (or delay 100))))))]
    (assoc proxy :consumer consumer)))

(defrecord SysoutEvictionChannel [this started]
  EvictionChannel
  (start [this ctx colls] (future nil))
  (started? [this] @(.started this))
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (println :insert coll-name unique-key))
  (update [this coll-name unique-key old-user-value new-user-value]
    (println :update coll-name unique-key))
  (delete [this coll-name unique-key]
    (println :delete coll-name unique-key))
  (delete-coll [this coll-name]
    (println :delete-coll coll-name)))

(defn create-SysoutEvictionChannel []
  (SysoutEvictionChannel. (atom false) (atom true)))
