(ns ^{:doc "Tests usecases on dynamically added constraints"}
  lambdaroyal.memory.eviction.test-couchdb-dynamic-collections
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


(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))
 

(facts "inserting and delete a card within the same tx, check that the card is deleted "
  (let [evictor (evict-couchdb/create)
        meta-model {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
          ;; delete the couchdb collection that will be added right after from previous attempts
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :person)))
        ;; insert a collection dynamically
        _ (add-collection ctx  {:name :person :evictor (-> @ctx :order :evictor) :evictor-delay 1000})
        _ (start-coll ctx :person)
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
       (insert tx :person 0 {:type :gaga :receiver :foo})
       (delete tx :person 0))
      (finally
        (.stop (-> @ctx :order :evictor))
        (-> @ctx :order :evictor :consumer deref)))
  ;;read again
  (let [evictor (evict-couchdb/create)
        meta-model {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (add-collection ctx  {:name :person :evictor (-> @ctx :order :evictor) :evictor-delay 1000})
        _ (start-coll ctx :person)
        _ (start-coll ctx :order)
        tx (create-tx ctx)
        _ (println :queue (-> @ctx :person :evictor .queue .size))]
    (try
      (fact "evictor must not reveal the document that was added and removed within the same tx"
            (select-first tx :order 0) => falsey)
      (finally
        (.stop (-> @ctx :order :evictor)))))))

