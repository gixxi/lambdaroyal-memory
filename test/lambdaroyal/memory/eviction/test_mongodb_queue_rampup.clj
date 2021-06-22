(ns lambdaroyal.memory.eviction.test-mongodb-queue-rampup
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.eviction.wal :as wal]
            [lambdaroyal.memory.eviction.mongodb :as evict-mongodb]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as db]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]
           [org.infobip.lib.popout FileQueue Serializer Deserializer WalFilesConfig CompressedFilesConfig]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

;; --------------------------------------------------------------------
;; HELPER FUNCTIONS
;; --------------------------------------------------------------------

(def db-name "dev-data1")

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

(defn drop-database [db-name]
  (let [url (evict-mongodb/get-database-url (System/getProperty "mongodb_preurl") (System/getProperty "mongodb_username") (System/getProperty "mongodb_password") (System/getProperty "mongodb_posturl"))
        conn (mg/connect-via-uri url)
        db (mg/get-db (conn :conn) db-name)]
    (db/drop-db db)))

;; --------------------------------------------------------------------
;; TESTS
;; --------------------------------------------------------------------

(let [_ (drop-database db-name)
      evictor (evict-mongodb/create :db-name db-name)
      meta-model
      {:stock {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
      ctx (create-context meta-model)
      conn (evict-mongodb/get-connection (-> @ctx :stock :evictor :eviction-channel :url))
      db (evict-mongodb/get-database (-> @ctx :stock :evictor :eviction-channel :db-name) conn)
      _ (start-coll ctx :stock)
      tx (create-tx ctx)]
  (try (gtid-dosync
        (doseq [x (range 10)]
          (insert tx :stock  x {:type :gaga :receiver :foo :run x})))
    (finally (.stop evictor)))
  (println "Finished first block"))

;; Manually adding records to simulate unfinished insertion
(let [queue (wal/create-queue "mongodb")
      _ (doseq [x (range 90 100)] (wal/insert-into-queue (wal/get-wal-payload :insert :stock x {:vlicRev 0
                                                                                                    :lagereigenschaft "Trocken"
                                                                                                    :vlicGtid 64945669975
                                                                                                    :creation-date "2021-06-14"
                                                                                                    :ident "Würfel, grün"
                                                                                                    :vlicKey 15
                                                                                                    :vlicUnique "asasa4hqXM5Ee"
                                                                                                    :vlicMru 1623659466138
                                                                                                    :lieferant "Baustoffe Ori"
                                                                                                    :vlicCreationTs 1623659466138}) queue))
      _ (.close queue)]
  (println "Finsihed second block"))

;; Start MongoDB evictor






