(ns ^{:doc "An Eviction Channel that uses a Couch DB server to load persistent data and to backup all transaction to. The couch db server
is supposed to run on http://localhost:5984 or as per JVM System Parameter -Dcouchdb.url or individually configured per eviction channel instance"} 
  lambdaroyal.memory.eviction.couchdb
  (require [lambdaroyal.memory.eviction.core :as evict]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.helper :refer :all]
           [com.ashafa.clutch :as clutch]
           [com.ashafa.clutch.utils :as utils]
           [clojure.string :as str]
           [clojure.java.io :as io]))

(defn- check-couchdb-connection [url]
  (if-not 
    (= "Welcome" (:couchdb (clutch/couchdb-info url)))
    (throw (IllegalArgumentException. (format "Cannot access Couch DB server on %s" url)))))

(defn- get-database-url [url db-name]
  (utils/url (utils/url url) db-name))

(defrecord CouchEvictionChannel [db]
  evict/EvictionChannel
  (stop [this] nil)
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (let [couchdb-doc-id (str unique-key)
          existing (clutch/get-document (.db this) couchdb-doc-id)
          doc (assoc user-value :_id (str unique-key) :unique-key unique-key)
          doc (if existing (assoc doc :_rev (:_rev existing)) doc)]
      (clutch/put-document (.db this) doc)))
  (update [this coll-name unique-key old-user-value new-user-value]
    (do
      (println :update)
      ))
  (delete [this coll-name unique-key]
    (do
      (println :delete)
      )))

(defn create 
  "provide custom url by calling this function with varargs :url \"https://username:password@account.cloudant.com\""
  [coll-name & args]
  (let [args (apply hash-map args)
        {:keys [url prefix] :or {url (or (System/getenv "couchdb.url") "http://localhost:5984")}} args
        _ (check-couchdb-connection url)
        db-name (if (keyword? coll-name) (name coll-name) name)
        db-name (if prefix (str prefix "_" db-name) db-name)
        db (clutch/get-database (get-database-url url db-name))]
    (CouchEvictionChannel. db)))


















