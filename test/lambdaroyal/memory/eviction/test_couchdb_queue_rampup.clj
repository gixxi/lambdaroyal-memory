(ns lambdaroyal.memory.eviction.test-couchdb-queue-rampup
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.eviction.wal :as wal]
            [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [com.ashafa.clutch :as clutch])
  (:import [lambdaroyal.memory.core ConstraintException]
           [org.infobip.lib.popout FileQueue Serializer Deserializer WalFilesConfig CompressedFilesConfig]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

;; --------------------------------------------------------------------
;; HELPER FUNCTIONS
;; --------------------------------------------------------------------

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

;; --------------------------------------------------------------------
;; TESTS
;; --------------------------------------------------------------------


(let [evictor (evict-couchdb/create)
      meta-model
      {:stock {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
      ctx (create-context meta-model)
       _ (clutch/delete-database (evict-couchdb/get-database-url (-> @ctx :stock :evictor :url) (name :stock)))
      _ (start-coll ctx :stock)
      tx (create-tx ctx)]
  (try
    (do
      (gtid-dosync
       (insert tx :stock  123456 {:type :gaga :receiver :foo :run 123459}))
      (Thread/sleep 2000))
    (finally (.stop evictor)))
  (println "Finished first block"))




;; Manually adding records to simulate unfinished insertion
(let [queue (wal/create-queue "couchdb")
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
      _ (println :close-queue)
      _ (.close queue)]
  (println "Finsihed second block"))

(let [evictor (evict-couchdb/create)
      meta-model
      {:stock {:unique true :indexes [] :evictor evictor :evictor-delay 1000}}
      ctx (create-context meta-model)
      _ (start-coll ctx :stock)
      tx (create-tx ctx)]
  (Thread/sleep 2000)
  (.stop evictor)
  (println "Finished third block"))






