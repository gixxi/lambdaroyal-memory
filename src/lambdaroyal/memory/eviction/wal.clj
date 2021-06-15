(ns ^{:doc "WAL - write ahead log, every transaction to the persistence layer is first flushed to local disk and asynchronously consumed by some thread that performs the atomic operation
1. peek from queue
2. write data to the persistence layer (e.g. mongodb)
3. poll from the queue in order to inform that the persistence layer is coherent"} 
  lambdaroyal.memory.eviction.wal
  (:require [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import [lambdaroyal.memory.core ConstraintException]
           [org.infobip.lib.popout FileQueue Serializer Deserializer WalFilesConfig CompressedFilesConfig]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat])
  (:gen-class))

(def verbose' (atom false))

(defn get-wal-payload "get some string that can be flushed to a wal file and be read later on"
  [fn coll-name id value]
  {:pre [(keyword fn) (contains? #{:insert :update :delete} fn) (keyword coll-name) (map? value)]}
  {:fn fn :coll coll-name :id id :val value})

(defn create-queue [prefix])

(defn queue [payload]
  (let [json' (json/generate-string payload)]
    nil) )

