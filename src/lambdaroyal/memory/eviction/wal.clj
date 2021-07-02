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
  {:fn fn :coll coll-name :id id :val value})

(defn create-queue [prefix]
  (let [thread-name (.getName (Thread/currentThread))
        wal-files-config-builder (WalFilesConfig/builder)
        wal-files-config-builder (.maxCount wal-files-config-builder (int 2048))
        wal-files-config-builder (.build wal-files-config-builder)
        compressed-files-config-builder (CompressedFilesConfig/builder)
        compressed-files-config-builder (.maxSizeBytes compressed-files-config-builder (* 1024 1024 16))
        compressed-files-config-builder (.build compressed-files-config-builder)

        queue (FileQueue/synced)
        queue (.name queue (str prefix "-evictor-queue"))
        queue (.folder queue (.getAbsolutePath (io/file (System/getProperty "java.io.tmpdir") "vlic/wal")))
        queue (.serializer queue Serializer/STRING)
        queue (.deserializer queue Deserializer/STRING)
        queue (.restoreFromDisk queue true)
        queue (.wal queue wal-files-config-builder)
        queue (.compressed queue compressed-files-config-builder)
        queue (.build queue)]
    (println "[create-queue] Queue was created in " thread-name)
    queue))

(defn insert-into-queue [payload queue]
  (let [json' (json/generate-string payload)]
    (.add queue json')))

(defn peek-queue [queue]
  (if queue (.peek queue)))

(defn process-queue 
  "Consumer function: will persist whats left in the WAL until condition @stopped-atom is met"
  [queue stopped-atom process-from-queue]
  (let [error-state (atom {:success true})
        _  (add-watch error-state :listener-one
                      (fn [key ref old-state new-state]
                        (cond
                          (and (-> old-state :success false?) (-> new-state :success true?))
                          (println "Error in processing queue" (:error-msg new-state))
                          (and (-> old-state :success true?) (-> new-state :success false?))
                          (println "Continue processing queue")
                          :else nil)))]
    (loop []
      (if-not @stopped-atom
        (try
          (if-let [queue-elem (.peek queue)]
            (do
              (reset! error-state (process-from-queue queue-elem))
              (let [result @error-state]
                (if (:success result)
                  (do
                    (println :success)
                    (.poll queue))
                  (println :error (:error-msg result))))
              (recur))
            (do
              (Thread/sleep 1000)
              (recur)))
          (catch Exception e (println "[process-queue] error-in-process" e)
                 (Thread/sleep 1000)
                 (recur)))
        (println "[process-queue] Process queue stopped")))))

(defn start-queue [queue stopped-atom process-from-queue]
  (if queue
    (do
      (println "[WAL start-queue]")
      (future
        (process-queue queue stopped-atom process-from-queue)))))

(defn loop-until-ok [code-func condition-not-ok-func error-msg sleep-time]
  (let [should-print-error (atom true)]
    (while (condition-not-ok-func)
      (if @should-print-error
        (do
          (println error-msg)
          (reset! should-print-error false)))
      (Thread/sleep sleep-time)))
  (code-func))

