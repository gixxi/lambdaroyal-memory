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
(reset! evict-couchdb/verbose false)

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

(defn rics [ctx source target] 
  (let [source-coll (get @ctx source)
        constraints (map last (-> source-coll :constraints deref))
        rics (filter
              #(instance? ReferrerIntegrityConstraint %) constraints)] 
              (some #(if (= (.foreign-coll %) target) %) rics)))
 

(defn merge-meta-models 
  "merges all meta-models into a single one one weaves in the std evictor and a std eviction delay of 1 sec"
  [evictor & maps]
  (apply merge
         (map 
          #(reduce (fn [acc [k v]]
                     (assoc acc k (assoc v :evictor-delay 1000 :evictor evictor)))
                   {} %)
          maps)))

;; (facts "inserting and delete a card within the same tx, check that the card is deleted "
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
;;         _ (start-coll ctx :order)
;;         tx (create-tx ctx)]
;;     (try
;;       (gtid-dosync
;;        (insert tx :order 0 {:type :gaga :receiver :foo})
;;        (delete tx :order 0))
;;       (finally
;;         (.stop (-> @ctx :order :evictor))
;;         (-> @ctx :order :evictor :consumer deref))))
;;   ;;read again
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;         tx (create-tx ctx)
;;         _ (println :queue (-> @ctx :order :evictor .queue .size))]
;;     (try
;;       (fact "evictor must not reveal the document that was added and removed within the same tx"
;;             (select-first tx :order 0) => falsey)
;;       (finally
;;         (.stop (-> @ctx :order :evictor))))))

;; (facts "inserting, update and delete a card within the same tx, check that the card is deleted "
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
;;         _ (start-coll ctx :order)
;;         tx (create-tx ctx)]
;;     (try
;;       (gtid-dosync
;;        (insert tx :order 0 {:type :gaga :receiver :foo})
;;        (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)
;;        (delete tx :order 0))
;;       (finally
;;         (.stop (-> @ctx :order :evictor))
;;         (-> @ctx :order :evictor :consumer deref))))
;;   ;;read again
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;         tx (create-tx ctx)
;;         _ (println :queue (-> @ctx :order :evictor .queue .size))]
;;     (try
;;       (fact "evictor must not reveal the document that was added and removed within the same tx"
;;             (select-first tx :order 0) => falsey)
;;       (finally
;;         (.stop (-> @ctx :order :evictor))))))


;; (facts 
;;  "inserting, update within the same tx, check that the card is revealed "
;;  (let [gtid' (atom nil)]
;;    (let [evictor (evict-couchdb/create)
;;          meta-model
;;          {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;          ctx (create-context meta-model)
;;          _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
;;          _ (start-coll ctx :order)
;;          tx (create-tx ctx)
;;          ]
;;      (try
;;        (gtid-dosync
;;         (insert tx :order 0 {:type :gaga :receiver :foo})
;;         (let [x (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)]
;;           (reset! gtid' (:vlicGtid x))
;;           (println :gtid' @gtid')
;;           (fact "gtid of object must not be nil" @gtid' => some?)))
       
;;        (finally
;;          (.stop (-> @ctx :order :evictor))
;;          (-> @ctx :order :evictor :consumer deref))))
;;    ;;read again
;;    (let [evictor (evict-couchdb/create)
;;          meta-model
;;          {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;          ctx (create-context meta-model)
;;          _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;          tx (create-tx ctx)
;;          _ (println :queue (-> @ctx :order :evictor .queue .size))]
;;      (try
;;        (let [x (select-first tx :order 0)]
;;          (println :x x)
;;          (check-coll ctx gtid')
;;          (fact "coll gtid must match mru object gtid after reread from couchdb" (-> ctx deref :order :gtid deref) => @gtid')
;;          (fact "evictor must not reveal the document that was added and removed within the same tx"
;;                x => truthy)
;;          (fact "evictor must not reveal the document that was added and removed within the same tx"
;;                (-> x last :type) => "gogo"))
;;        (finally
;;          (.stop (-> @ctx :order :evictor)))))))

;; (facts "inserting within tx, update and delete a card within tx', check that the card is deleted "
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
;;         _ (start-coll ctx :order)
;;         tx (create-tx ctx)]
;;     (try
;;       (do
;;         (gtid-dosync
;;          (insert tx :order 0 {:type :gaga :receiver :foo}))
;;         (gtid-dosync
;;          (alter-document tx :order (select-first tx :order 0) assoc :type :gogo)
;;          (delete tx :order 0)))
;;       (finally
;;         (.stop (-> @ctx :order :evictor))
;;         (-> @ctx :order :evictor :consumer deref))))
;;   ;;read again
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;         tx (create-tx ctx)
;;         _ (println :queue (-> @ctx :order :evictor .queue .size))]
;;     (try
;;       (fact "evictor must not reveal the document that was added and removed within the same tx"
;;             (select-first tx :order 0) => falsey)
;;       (finally
;;         (.stop (-> @ctx :order :evictor))))))


;; (facts "inserting within tx, reload, update and delete a card within tx', check that the card is deleted "
;;   (let [evictor (evict-couchdb/create)
;;         _ (reset! evict/verbose' false)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :order :evictor :url) (name :order)))
;;         _ (start-coll ctx :order)
;;         tx (create-tx ctx)]
;;     (try
;;       (gtid-dosync
;;        (insert tx :order 0 {:type :gaga :receiver :foo}))
;;       (finally
;;         (.stop (-> @ctx :order :evictor))
;;         (-> @ctx :order :evictor :consumer deref))))
;;   (println :finish 1)
;;   ;;read again
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;         tx (create-tx ctx)
;;         _ (println :queue (-> @ctx :order :evictor .queue .size))]
;;     (try
;;       (gtid-dosync
;;        (do
;;          (comment (alter-document tx :order (select-first tx :order 0) assoc :type :gogo))
;;          (delete tx :order 0)))
;;       (finally
;;         (.stop (-> @ctx :order :evictor))
;;         (-> @ctx :order :evictor :consumer deref))))
;;   (println :finish 2)
;;   (let [evictor (evict-couchdb/create)
;;         meta-model
;;         {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;         ctx (create-context meta-model)
;;         _ @(.start (-> @ctx :order :evictor) ctx [(:order @ctx)])
;;         tx (create-tx ctx)
;;         _ (println :queue (-> @ctx :order :evictor .queue .size))
;;         _ (evict-couchdb/schedule-compaction evictor ctx)]
;;     (try
;;       (fact "evictor must not reveal the document that was updated and removed within the same tx"
;;             (select-first tx :order 0) => falsey)
;;       (finally
;;         (do
;;         (Thread/sleep 5000)
;;         (.stop (-> @ctx :order :evictor)))))))

