(ns lambdaroyal.memory.migration.mongodb
  (:require [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :as context]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.mongodb :as evict-mongodb]
            [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
            [lambdaroyal.memory.helper :refer :all]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

;; --------------------------------------------------------------------
;; HELPER FUNCTIONS
;; --------------------------------------------------------------------


;; --------------------------------------------------------------------
;; 
;; --------------------------------------------------------------------

(defn migrate-to-target-channel
  "targetChannel is supposed to have a put-batch function with the parameters [channel coll-name unique-key user-value]"
  [ctx put-batch-lambda]
  (let [state (atom {:collections-done [] :finished false})]
    (future (let [tx (create-tx ctx)
                  res  (dosync (reduce
                                (fn [acc coll]
                                  (conj acc [coll (doall (select tx coll))]))
                                ;; empty accumulator - we add on constantly
                                []
                                ;; we iterate over this - coll is one element
                                (keys @ctx)))

                  ;; I/O
                  _ (println :count-res (count res))
                  _   (doseq [[coll records] res]
                        (println :coll coll)
                        (put-batch-lambda coll records)
                        (reset! state (update @state :collections-done conj coll)))
                  _ (println "finished with all collections")
                  _ (reset! state (update @state :finished not))
                  _ (println :final-state @state)]))
    state))



(defn migrate-to-mongoDB [ctx & [mongo-dbname]]
  (let [dbname (or mongo-dbname (System/getProperty "mongodb_dbname"))
        _ (println :dbname dbname)
        evictor-channel (evict-mongodb/create :db-name dbname)
        meta-model {:order {:unique true :indexes [] :evictor evictor-channel :evictor-delay 1000}}
        mongodb-ctx (context/create-context meta-model)
        conn (evict-mongodb/get-connection (-> @mongodb-ctx :order :evictor :eviction-channel :url))
        db (evict-mongodb/get-database (-> @mongodb-ctx :order :evictor :eviction-channel :db-name) conn)
        _ (println :db-in-migrate-to-MongoDB db)
        state (migrate-to-target-channel ctx  (fn [coll records] (evict-mongodb/put-batch evictor-channel coll records db)))]
    state))

(comment (defn migrate-to-couchDB [ctx targethannel db]
           (let [state (migrate-to-target-channel ctx  (fn [coll records] (evict-couchdb/put-batch target-channel coll records db)))]
             state)
           ))
