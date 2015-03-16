(ns lambdaroyal.memory.eviction.test-couchdb
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.eviction.core :as evict]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
           [lambdaroyal.memory.helper :refer :all]
           [com.ashafa.clutch :as clutch])
  (import [lambdaroyal.memory.core ConstraintException]
          [org.apache.log4j BasicConfigurator]))

(BasicConfigurator/configure)

(facts "checking state model on the couch db eviction channel"
  (let [meta-model
        {:order {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 10}}
        ctx (create-context meta-model)]
    (fact "cannot start calling user-scope functions until eviction channel is not started" (create-tx ctx) => (throws IllegalStateException))))

(facts "checking state model on the couch db eviction channel"
  (let [meta-model
        {:order {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 10}}
        ctx (create-context meta-model)
     _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])]
    (fact "cannot start calling user-scope functions until eviction channel is not started" (create-tx ctx) => truthy)))

(facts "inserting into an empty couch db instance"
  (let [meta-model
        {:order {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
        tx (create-tx ctx)]
    (try
      (let [_
            (dosync
             (doseq [r (range 100)]
               (insert tx :order r {:type :gaga :receiver :foo :run r})))
            inserted (select tx :order >= 0)]
        (dosync
         (doseq [d inserted]
           (alter-document tx :order d assoc :run 1)))
        (dosync
         (doseq [d (drop 50 inserted)]
           (delete tx :order (first d)))))
      (finally
        (.stop (-> @ctx :order :evictor))
        (-> @ctx :order :evictor :consumer deref))))
  ;;read again
  (let [meta-model
        {:order {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
        tx (create-tx ctx)
        _ (println :queue (-> @ctx :order :evictor .queue .size))]
    (try
      (let [inserted
            (dosync
             (select tx :order >= 0))]
        (fact "must reload all documents during ramp-up"
          (apply + (map #(-> % last :run) inserted)) => 50)
        (fact "couchdb must reflect the deletion of 50 out of 100 documents" (count (select tx :order >= 0)) => 50))
      (finally
        (.stop (-> @ctx :order :evictor))
        (-> @ctx :order :evictor :consumer deref)
        (fact "couchdb must reflect the deletion of 50 out of 100 documents - also after the eviction channel stopped" (count (select tx :order >= 0)) => 50))))
  ;;do masstest
  (fact "timing, adding 1000 to couch db and removing 500 afterwards must not take more than 10 secs"
    (first (timed (let [meta-model
                        {:order {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 500}}
                        ctx (create-context meta-model)
                        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
                        _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
                        tx (create-tx ctx)]
                    (try
                      (let [_
                            (fact "inserting into mem db" (first (timed (dosync
                                                                         (doseq [r (range 1000)]
                                                                           (insert tx :order r {:type :gaga :receiver :foo :run r :interface :yes :live true}))))) => (roughly 0 200))
                            inserted (select tx :order >= 0)]
                        (dosync
                         (doseq [d (drop 500 inserted)]
                           (delete tx :order (first d)))))
                      (finally
                        (.stop (-> @ctx :order :evictor))
                        (-> @ctx :order :evictor :consumer deref)))))) => (roughly 0 30000)))











