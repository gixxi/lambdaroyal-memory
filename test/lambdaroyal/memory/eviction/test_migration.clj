(ns lambdaroyal.memory.eviction.test-migration
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.mongodb :as evict-mongodb]
            [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
            [lambdaroyal.memory.helper :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as md]
            [com.ashafa.clutch :as clutch]
            [cheshire.core :as json]
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
(reset! evict-couchdb/verbose true)

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

(defn get-database-url [prefix-url username password postfix-url]
  (str prefix-url username ":" password "@" postfix-url))

(defn get-database [db-name conn]
  (mg/get-db (conn :conn) db-name))

(defn get-connection [url]
  (mg/connect-via-uri url))

(def mongodb-dbname "migrationTest")

;; --------------------------------------------------------------------
;; TESTS
;; --------------------------------------------------------------------

(let [_ (clutch/delete-database "http://localhost:5984/order")
      evictor (evict-couchdb/create)
      meta-model
      {:order {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
      ctx (create-context meta-model)
      _ (start-coll ctx :order)
      tx (create-tx ctx)
      url (evict-mongodb/get-database-url (System/getProperty "mongodb_preurl") (System/getProperty "mongodb_username") (System/getProperty "mongodb_password") (System/getProperty "mongodb_posturl"))
      conn (evict-mongodb/get-connection url)
      db (evict-mongodb/get-database mongodb-dbname conn)]
  (try
    (let [_
          (dosync
           (doseq [r (range 100)]
             (insert tx :order r {:type :gaga :receiver :foo :run r})))
           ;; We get the data in a transaction to get a consistent view of the data
          res (reduce (fn [acc [coll xs]] (assoc acc coll xs)) {} 
                      (dosync (reduce fn [acc coll]
                                (let [records (select tx coll-key)]))))

          res  (dosync (doseq [coll-key (keys @ctx)]
                       (let [records (select tx coll-key)]
                         
                         )))
          
          _   (dosync (doseq [coll-key (keys @ctx)]
                        (let [records (select tx coll-key)
                              json' (map #(assoc (last %) :_id (first %)) records)
                              _ (println :record json')
                              _ (mc/insert-batch db coll-key json')])))])))
       
       