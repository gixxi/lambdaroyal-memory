(ns ^{:doc "WAL - write ahead log, every transaction to the persistence layer is first flushed to local disk and asynchronously consumed by some thread that performs the atomic operation
1. peek from queue
2. write data to the persistence layer (e.g. mongodb)
3. poll from the queue in order to inform that the persistence layer is coherent"}
 lambdaroyal.memory.eviction.wal
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]
           [org.infobip.lib.popout FileQueue Serializer Deserializer WalFilesConfig CompressedFilesConfig]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat])
  (:gen-class))

(def verbose' (atom false))

(defn get-wal-payload "get some string that can be flushed to a wal file and be read later on"
  [fn coll-name id value]
  {:pre [(keyword fn) (contains? #{:insert :update :delete} fn) (keyword coll-name) (map? value)]}
  {:fn fn :coll coll-name :id id :val value :timestamp (System/currentTimeMillis)})

(defn create-queue [prefix]
  (let [wal-files-config-builder (WalFilesConfig/builder)
        wal-files-config-builder (.maxCount wal-files-config-builder (int 2048))
        wal-files-config-builder (.build wal-files-config-builder)

        compressed-files-config-builder (CompressedFilesConfig/builder)
        compressed-files-config-builder (.maxSizeBytes compressed-files-config-builder (* 1024 1024 16))
        compressed-files-config-builder (.build compressed-files-config-builder)

        queue (FileQueue/synced)
        queue (.name queue (str prefix "evictor-queue"))
        queue (.folder queue (.getAbsolutePath (io/file (System/getProperty "java.io.tmpdir") "vlic/wal")))
        queue (.serializer queue Serializer/STRING)
        queue (.deserializer queue Deserializer/STRING)
        queue (.restoreFromDisk queue true)
        queue (.wal queue wal-files-config-builder)
        queue (.compressed queue compressed-files-config-builder)
        queue (print-info-timed "Building the queue" (.build queue))]
    queue))

(defn insert-into-queue [payload queue]
  (let [json' (json/generate-string payload)]
    (.add queue json')))

(defn peek-queue [queue]
  (if queue (.peek queue)))

(defn process-queue [queue stopped-fn process-from-queue]
  (loop []
    (if-let [queue-elem (.peek queue)]
      (let [result (process-from-queue queue-elem)]
        (if (:success result)
          (.pop queue)
          (do
            (Thread/sleep 1000)
                              ;; LOG ERROR HERE Raise Exception ONCE
            )))
      (do
        (Thread/sleep 1000)
        (if (stopped-fn) (println :queue-check-stopped) (recur))))))

(defn start-queue [queue stopped-fn process-from-queue]
  (if queue (future
              (process-from-queue queue stopped-fn process-from-queue))))

(defn leftover-from-val "It checks if the queue is empty. if so it does nothing. If not empty we emit some log info." [])

(defn loop-until-ok [code-func condition-not-ok-func error-msg sleep-time]
  (while (condition-not-ok-func)
    (println error-msg)
    (Thread/sleep sleep-time))
  (code-func))

