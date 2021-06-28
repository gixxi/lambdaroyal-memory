(ns lambdaroyal.memory.eviction.test-couchdb-stm-reduction
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

(defn merge-meta-models
  "merges all meta-models into a single one one weaves in the std evictor and a std eviction delay of 1 sec"
  [evictor & maps]
  (apply merge
         (map
          #(reduce (fn [acc [k v]]
                     (assoc acc k (assoc v :evictor-delay 1000 :evictor evictor)))
                   {} %)
          maps)))


(facts "inserting, update and delete a card and insert it again within the same tx, check that the card is revealed "
       (let [evictor (evict-couchdb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
             _ (start-coll ctx :order)
             tx (create-tx ctx)]
         (try
           (gtid-dosync
            (insert tx :order 0 {:type :gaga :receiver :foo})
            (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)
            (delete tx :order 0)
            (insert tx :order 0 {:type :gugus :receiver :foo}))
           (finally
             (.stop (-> @ctx :order :evictor))
             (-> @ctx :order :evictor :consumer deref))))
  ;;read again
       (let [evictor (evict-couchdb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
             tx (create-tx ctx)
             _ (println :queue (-> @ctx :order :evictor .queue .size))]
         (try
           (let [x (select-first tx :order 0)]
             (doseq [x (select tx :order)]
               (println :order-in-doseq x))
             (fact "evictor must not reveal the document that was added and removed within the same tx"
                   x => truthy)
             (fact "evictor must not reveal the document that was added and removed within the same tx"
                   (-> x last :type) => "gugus"))
           (finally
             (.stop (-> @ctx :order :evictor))))))

(defn check-coll [ctx gtid]
  (-> ctx deref :order :gtid deref))

