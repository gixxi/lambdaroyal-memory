(ns lambdaroyal.memory.eviction.test-couchdb-insert-into-empty-instance
  "Inserts 100 records, updates all of them and deletes the first 50 elements, the ne check the last 50 elements"
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

(defn rics [ctx source target]
  (let [source-coll (get @ctx source)
        constraints (map last (-> source-coll :constraints deref))
        rics (filter
              #(instance? ReferrerIntegrityConstraint %) constraints)]
    (some #(if (= (.foreign-coll %) target) %) rics)))

(facts "inserting into an empty couch db instance"
       (let [ _ (clutch/delete-database "http://localhost:5984/order")
             evictor (evict-couchdb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             _ (start-coll ctx :order)
             tx (create-tx ctx)]
         (try
           (let [_
                 (gtid-dosync
                  (doseq [r (range 100)]
                    (insert tx :order r {:type :gaga :receiver :foo :run r})))
                 inserted (select tx :order >= 0)]
             (gtid-dosync
              (doseq [d inserted]
                (alter-document tx :order d assoc :run 1)))
             (gtid-dosync
              (doseq [d (drop 50 inserted)]
                (delete tx :order (first d))))
        ;; delete the couchdb collection that will be added right after from previous attempts
             (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :part-order)))
             (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :line-item)))
        ;; insert a collection dynamically
             (let [new-collection (add-collection ctx  {:name :part-order :evictor (-> @ctx :order :evictor) :evictor-delay 1000
                                                        :foreign-key-constraints [{:name :order :foreign-coll :order :foreign-key :order}]})]
               (start-coll ctx :part-order))
             _ (let [new-collection (add-collection ctx  {:name :line-item :evictor (-> @ctx :order :evictor) :evictor-delay 1000
                                                          :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]})]
                 (start-coll ctx :line-item))
        ;; insert to the new collection
             (gtid-dosync
              (insert (create-tx ctx) :part-order 1 {:type :gaga :order 1 :receiver :foo :run 2})
              (insert (create-tx ctx) :line-item 1 {:part-order 1})))
           (finally
             (.stop (-> @ctx :order :evictor))
             (.stop (-> @ctx :part-order :evictor))
             (-> @ctx :order :evictor :consumer deref))))

  ;;read again
       
       
       (let [evictor (evict-couchdb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
        ;; insert the collection dynamically (again)
             _ (let [new-collection (add-collection ctx  {:name :part-order :evictor evictor :evictor-delay 1000
                                                          :foreign-key-constraints [{:name :order :foreign-coll :order :foreign-key :order}]})]
                 (start-coll ctx :part-order))
             _ (let [new-collection (add-collection ctx  {:name :line-item :evictor evictor :evictor-delay 1000
                                                          :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]})]
                 (start-coll ctx :line-item))
             tx (create-tx ctx)
             _ (println :queue (-> @ctx :order :evictor .queue .size))]
         (try
           (let [inserted
                 (gtid-dosync
                  (select tx :order >= 0))]
             (fact "evictor must reveal the document of the dynamically add collection"
                   (select-first tx :part-order 1) => truthy)
             (fact "deleting the part-order must delete the RIC from referencing coll :line-item"
                   (do
                     (delete-collection ctx :part-order)
                     (rics ctx :line-item :part-order)) => nil)
             (fact "must reload all documents during ramp-up"
                   (apply + (map #(-> % last :run) inserted)) => 50)
             (fact "couchdb must reflect the deletion of 50 out of 100 documents" (count (select tx :order >= 0)) => 50))
           (finally
             (.stop (-> @ctx :order :evictor))
             (-> @ctx :order :evictor :consumer deref)
             (fact "couchdb must reflect the deletion of 50 out of 100 documents - also after the eviction channel stopped" (count (select tx :order >= 0)) => 50))))
          )