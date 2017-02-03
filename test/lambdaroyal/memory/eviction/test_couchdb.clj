(ns lambdaroyal.memory.eviction.test-couchdb
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

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

(defn rics [ctx source target] 
  (let [source-coll (get @ctx source)
        constraints (map last (-> source-coll :constraints deref))
        rics (filter
              #(instance? ReferrerIntegrityConstraint %) constraints)] 
              (some #(if (= (.foreign-coll %) target) %) rics)))
 
(facts "inserting into an empty couch db instance"
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
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
        (dosync 
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
            (dosync
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
  ;;do masstest
  (fact "timing, adding 1000 to couch db and removing 500 afterwards must not take more than 10 secs"
    (first (timed (let [meta-model
                        {:order {:unique true :indexes [] :evictor (evict-couchdb/create) :evictor-delay 500}}
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

(defn merge-meta-models 
  "merges all meta-models into a single one one weaves in the std evictor and a std eviction delay of 1 sec"
  [evictor & maps]
  (apply merge
         (map 
          #(reduce (fn [acc [k v]]
                     (assoc acc k (assoc v :evictor-delay 1000 :evictor evictor)))
                   {} %)
          maps)))

(facts "inserting and delete a card within the same tx, check that the card is deleted "
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
       (insert tx :order 0 {:type :gaga :receiver :foo})
       (delete tx :order 0))
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
      (fact "evictor must not reveal the document that was added and removed within the same tx"
            (select-first tx :order 0) => falsey)
      (finally
        (.stop (-> @ctx :order :evictor))))))

(facts "inserting, update and delete a card within the same tx, check that the card is deleted "
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
       (insert tx :order 0 {:type :gaga :receiver :foo})
       (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)
       (delete tx :order 0))
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
      (fact "evictor must not reveal the document that was added and removed within the same tx"
            (select-first tx :order 0) => falsey)
      (finally
        (.stop (-> @ctx :order :evictor))))))

(facts "inserting, update and delete a card and insert it again within the same tx, check that the card is revealed "
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
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
        (fact "evictor must not reveal the document that was added and removed within the same tx"
             x => truthy)
        (fact "evictor must not reveal the document that was added and removed within the same tx"
              (-> x last :type) => "gugus"))
      (finally
        (.stop (-> @ctx :order :evictor))))))

(facts "inserting, update within the same tx, check that the card is revealed "
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
       (insert tx :order 0 {:type :gaga :receiver :foo})
       (alter-document tx :order (select-first tx :order 0) assoc :type :gogo))
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
        (fact "evictor must not reveal the document that was added and removed within the same tx"
             x => truthy)
        (fact "evictor must not reveal the document that was added and removed within the same tx"
              (-> x last :type) => "gogo"))
      (finally
        (.stop (-> @ctx :order :evictor))))))

(facts "inserting within tx, update and delete a card within tx', check that the card is deleted "
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (do
        (dosync
         (insert tx :order 0 {:type :gaga :receiver :foo}))
        (dosync
         (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)
         (delete tx :order 0)))
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
      (fact "evictor must not reveal the document that was added and removed within the same tx"
            (select-first tx :order 0) => falsey)
      (finally
        (.stop (-> @ctx :order :evictor))))))

(facts "inserting within tx, reload, update and delete a card within tx', check that the card is deleted "
  (let [evictor (evict-couchdb/create)
        _ (reset! evict/verbose' false)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
        _ (start-coll ctx :order)
        tx (create-tx ctx)]
    (try
      (dosync
       (insert tx :order 0 {:type :gaga :receiver :foo}))
      (finally
        (.stop (-> @ctx :order :evictor))
        (-> @ctx :order :evictor :consumer deref))))
  (println :finish 1)
  ;;read again
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
        tx (create-tx ctx)
        _ (println :queue (-> @ctx :order :evictor .queue .size))]
    (try
      (dosync
       (do
         (comment (alter-document tx :order (select-first tx :order 0) assoc :type :gogo))
         (delete tx :order 0)))
      (finally
        (.stop (-> @ctx :order :evictor))
        (-> @ctx :order :evictor :consumer deref))))
  (println :finish 2)
  (let [evictor (evict-couchdb/create)
        meta-model
        {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
        ctx (create-context meta-model)
        _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
        tx (create-tx ctx)
        _ (println :queue (-> @ctx :order :evictor .queue .size))]
    (try
      (fact "evictor must not reveal the document that was updated and removed within the same tx"
            (select-first tx :order 0) => falsey)
      (finally
        (.stop (-> @ctx :order :evictor))))))
