(ns lambdaroyal.memory.eviction.test-mongodb
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.mongodb :as evict-mongodb]
            [lambdaroyal.memory.helper :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as md]
             [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

;; --------------------------------------------------------------------
;; HELPER FUNCTIONS
;; --------------------------------------------------------------------


(reset! evict/verbose' true)

(reset! evict-mongodb/verbose true)

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

;; --------------------------------------------------------------------
;; TESTS
;; --------------------------------------------------------------------

(facts "Insert one record into Mongdb and check if its in memory and DB"
       (let [evictor (evict-mongodb/create)
             meta-model
             {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
             ctx (create-context meta-model)
             conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
             db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
             _ (mc/remove db :order)
             _ (start-coll ctx :order)
             tx (create-tx ctx)]
         (try
           (do
             (gtid-dosync
              (insert tx :order  123456 {:type :gaga :receiver :foo :run 123459}))
             (Thread/sleep 2000)
             (fact "It will return the inserted record" (count (mc/find-maps db :order {:_id 123456})) => 1))))

;; (facts "Update one record 1000 times and check if the value in MongoDB matches the in-memory value and also checks it completes under 10 seconds"
;;        (let [evictor (evict-mongodb/create)
;;              meta-model
;;              {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;              ctx (create-context meta-model)
;;              conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;              db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;              _ (mc/remove db :order)
;;              _ (start-coll ctx :order)
;;              tx (create-tx ctx)]
;;          (try
;;            (do
;;              (dosync
;;               (insert tx :order 123456 {:type :gaga :receiver :foo :run 123456}))
;;              (Thread/sleep 2000)
;;              (doseq [r (range 10)]
;;                (dosync
;;                 (alter-document tx :order (select-first tx :order 123456) assoc :run r)))
;;              (check-for-existence db :order :run 9 5000))
;;            (finally
;;              (println :finish)
;;              (.stop (-> @ctx :order :evictor))))))

;; ;; Update 1000 records once 
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)]
;;   (try
;;     (do
;;       (let [_ (dosync
;;                (doseq [r (range 1000)]
;;                  (insert tx :order r {:type :gaga :receiver :foo :run r})))
;;             result (timed (dosync (doseq [r (range 1000)]
;;                                     (alter-document tx :order (select-first tx :order r) assoc :type (str "gaga-" r)))))]
;;         (append-to-timeseries (System/getProperty "perftest-username") "Test" (first result))))
;;     (finally
;;       (println :finish)
;;       (.stop (-> @ctx :order :evictor)))))


;; ;; Create 1000 inserts Update each 3 times and Delete
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)
;;       amount 1000]
;;   (try
;;     (do
;;       (let [_ (dosync
;;                (doseq [r (range amount)]
;;                  (insert tx :order r {:type :gaga :receiver :foo :run r})))
;;             _ (dosync (doseq [r (range amount)]
;;                         (alter-document tx :order (select-first tx :order r) assoc :type (str "first-update-" r)))
;;                       (doseq [r (range amount)]
;;                         (alter-document tx :order (select-first tx :order r) assoc :type (str "second-update-" r)))
;;                       (doseq [r (range amount)]
;;                         (alter-document tx :order (select-first tx :order r) assoc :type (str "third-update" r))))
;;             _ (dosync (doseq [r (range amount)]
;;                         (delete tx :order {:_id r})))]))
;;     (finally
;;       (println :finish)
;;       (.stop (-> @ctx :order :evictor)))))

;; ;; Update 100 records once, do it in 5 transactions
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)]
;;   (try
;;     (do
;;       (let [_ (dosync
;;                (doseq [r (range 100)]
;;                  (insert tx :order r {:type :gaga :receiver :foo :run r})))
;;             _ (dosync
;;                (doseq [r (range 0 20)]
;;                  (alter-document tx :order (select-first tx :order r) assoc :type (str "update" r))))
;;             _ (dosync
;;                (doseq [r (range 20 40)]
;;                  (alter-document tx :order (select-first tx :order r) assoc :type (str "update" r))))
;;             _ (dosync
;;                (doseq [r (range 40 60)]
;;                  (alter-document tx :order (select-first tx :order r) assoc :type (str "update" r))))
;;             _ (dosync
;;                (doseq [r (range 60 80)]
;;                  (alter-document tx :order (select-first tx :order r) assoc :type (str "update" r))))
;;             _ (dosync
;;                (doseq [r (range 80 100)]
;;                  (alter-document tx :order (select-first tx :order r) assoc :type (str "update" r))))
;;             ]))
;;     (finally
;;       (println :finish)
;;       (.stop (-> @ctx :order :evictor)))))


 
;; ;; Insert 100 records
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn    (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)]
;;   (try
;;     (let [_ (gtid-dosync
;;              (doseq [r (range 100)]
;;                (insert tx :order r {:type :gaga :receiver :foo :run r})))
;;           inserted (select tx :order >= 0)])))

;;   (try
;;     (let [_ (.stop (-> @ctx :order :evictor))
;;           _ (start-coll ctx :order)]))


;; ;; Update one record
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)]
;;   (try
;;     (gtid-dosync
;;      (insert tx :order "123456" {:type :gaga :receiver :foo :run "123456"})
;;      (alter-document tx :order (select-first tx :order "123456") assoc :run "191919191"))))

;; ;; Delete whole coll
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)])

;; ;; Delete One
;; (let [evictor (evict-mongodb/create)
;;       meta-model
;;       {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
;;       ctx (create-context meta-model)
;;       conn (evict-mongodb/get-connection (-> @ctx :order :evictor :eviction-channel :url))
;;       db (evict-mongodb/get-database (-> @ctx :order :evictor :eviction-channel :db-name) conn)
;;       _ (mc/remove db :order)
;;       _ (start-coll ctx :order)
;;       tx (create-tx ctx)]
;;   (try
;;     (gtid-dosync
;;      (insert tx :order "123456" {:type :gaga :receiver :foo :run "123456"})
;;      (delete tx :order (-> (select-first tx :order "123456") first)))))
