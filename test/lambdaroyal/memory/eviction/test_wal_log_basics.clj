(ns lambdaroyal.memory.eviction.test-wal-log-basics
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
           [org.infobip.lib.popout FileQueue Serializer Deserializer WalFilesConfig CompressedFilesConfig]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

(BasicConfigurator/configure)


(def queue (let [wal-files-config-builder (WalFilesConfig/builder)
           wal-files-config-builder (.maxCount wal-files-config-builder (int 2048))
           wal-files-config-builder (.build wal-files-config-builder)

           compressed-files-config-builder (CompressedFilesConfig/builder)
           compressed-files-config-builder (.maxSizeBytes compressed-files-config-builder (* 1024 1024 16))
           compressed-files-config-builder (.build compressed-files-config-builder)

           queue (FileQueue/synced)
           queue (.name queue "couchdb-evictor-queue")
           queue (.folder queue (.getAbsolutePath (clojure.java.io/file (System/getProperty "java.io.tmpdir") "vlic/wal")))
           queue (.serializer queue Serializer/STRING)
           queue (.deserializer queue Deserializer/STRING)
           queue (.restoreFromDisk queue true)
           queue (.wal queue wal-files-config-builder)
           queue (.compressed queue compressed-files-config-builder)
           queue (.build queue)]
             queue))



(comment (.add queue "faris")
         (.add queue "christian")
         (.poll queue)

         (def doc "{\"_id\":{\"$numberLong\":\"3964\"},\"amount\":{\"$numberInt\":\"6\"},\"vlicUser\":null,\"vlicCreatedBy\":null,\"vlicRev\":{\"$numberLong\":\"0\"},\"article\":{\"$numberLong\":\"1332\"},\"bestBefore\":\"20161014T000000.000Z\",\"batch\":\"CFI258\",\"vlicGtid\":{\"$numberLong\":\"64616772785\"},\"uom\":\"STK\",\"luType\":\"Bag\",\"creation-date\":\"2021-06-10\",\"ident\":\"3964\",\"vlicKey\":{\"$numberLong\":\"3964\"},\"origAmount\":{\"$numberLong\":\"7\"},\"location\":{\"$numberLong\":\"3964\"},\"vlicUnique\":\"zXV352ubu\",\"vlicMru\":{\"$numberLong\":\"1623330519612\"},\"vlicCreationTs\":{\"$numberLong\":\"1623330519612\"}}")

         (time (doseq [x (range 1024)]
                 (.add queue doc)))

         (doseq [x (range 256)]
           (println (.poll queue))))

(println :done (.peek queue))

