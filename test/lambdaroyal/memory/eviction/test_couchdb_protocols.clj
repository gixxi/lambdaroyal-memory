(ns lambdaroyal.memory.eviction.test-couchdb-protocols
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
            [lambdaroyal.memory.helper :refer :all]
            [com.ashafa.clutch :as clutch])
  (:import [lambdaroyal.memory.core ConstraintException]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]))

(BasicConfigurator/configure)
(reset! evict-couchdb/verbose false)

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

(facts "Check if couchdb implments certain protocols"
       (let [evictor (evict-couchdb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
             _ (start-coll ctx :order)
             tx (create-tx ctx)]

    ;;read again
         (try
           (fact "evictor must implement protocol"
                 (satisfies? evict/EvictionChannelHeartbeat evictor) => true)
           (fact "evictor must be alive"
                 (.alive? evictor) => true)
           (finally
             (.stop (-> @ctx :order :evictor))))))

(facts "checking state model on the couch db eviction channel"
       (let [meta-model
             {:order {:unique true :indexes [] :evictor (evict-couchdb/create) :evictor-delay 10}}
             ctx (create-context meta-model)]
         (fact "cannot start calling user-scope functions until eviction channel is not started" (create-tx ctx) => (throws IllegalStateException))))

(facts "checking state model on the couch db eviction channel"
       (let [meta-model
             {:order {:indexes [] :evictor (evict-couchdb/create) :evictor-delay 10}}
             ctx (create-context meta-model)
             _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])]
         (fact "cannot start calling user-scope functions until eviction channel is not started" (create-tx ctx) => truthy)))