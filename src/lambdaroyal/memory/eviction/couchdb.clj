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
           [clojure.java.io :as io]
           [clojure.tools.logging :as log]))

(defn- check-couchdb-connection [url]
  (if-not 
    (= "Welcome" (:couchdb (clutch/couchdb-info url)))
    (throw (IllegalArgumentException. (format "Cannot access Couch DB server on %s" url)))))

(defn- get-database-url [url db-name]
  (utils/url (utils/url url) db-name))

(defn- is-update-conflict [e]
  (try
    (boolean (re-find #"update conflict" (-> e .getData :object :body)))
    (catch Exception e false)))

(defn- put-document 
  "inserts a document to the couchdb instance referenced by the CouchEvictionChannel [channel]. if a version conflict occurs then this function trys"
  [channel coll-name unique-key user-value]
  (let [rev-key [coll-name unique-key]
        rev (get @(:revs channel) rev-key)
        doc (assoc user-value :_id (str unique-key) :unique-key unique-key)
        doc (if rev (assoc doc :_rev rev) doc)
        put (fn [doc] (let [result (clutch/put-document (.db channel) doc)]
                        (swap! (.revs channel) assoc rev-key (:_rev result))))]
    (try
      (put doc)
      (catch Exception e 
        ;;retry
        (let [_ (if (is-update-conflict e)
                  (do
                    (println (format "update conflict on %s. try again." doc))
                    (log/warn (format "update conflict on %s. try again." doc))))
              existing (clutch/get-document (.db channel) (str unique-key))
              doc (assoc doc :_rev (:_rev existing))]
          (try
            (put doc)
            (catch Exception e
              (do (println (format "failed to put document %s to couchdb during retry. stop eviction channel." doc)))
              (do (log/fatal (format "failed to put document %s to couchdb during retry. stop eviction channel." doc)))
              (.stop channel))))))))

(defn- delete-document 
  "deletes a document from the couchdb"
  [channel coll-name unique-key]
  (if-let [existing (clutch/get-document (.db channel) (str unique-key))]
    (try
      (do
        (clutch/delete-document (.db channel) existing)
        (swap! (.revs channel) dissoc [coll-name unique-key]))
      (catch Exception e
        (do (println (format "failed to delete %s from couchdb. stop eviction channel." existing)))
        (do (log/fatal (format "failed to delete document %s from couchdb. stop eviction channel." existing)))
        (.stop channel)))))

(defrecord CouchEvictionChannel [db revs]
  evict/EvictionChannel
  (stop [this] nil)
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (put-document this coll-name unique-key user-value))
  (update [this coll-name unique-key old-user-value new-user-value]
    (put-document this coll-name unique-key new-user-value))
  (delete [this coll-name unique-key]
    (delete-document this coll-name unique-key)
   ))

(defn create 
  "provide custom url by calling this function with varargs :url \"https://username:password@account.cloudant.com\""
  [coll-name & args]
  (let [args (apply hash-map args)
        {:keys [url prefix] :or {url (or (System/getenv "couchdb.url") "http://localhost:5984")}} args
        _ (check-couchdb-connection url)
        db-name (if (keyword? coll-name) (name coll-name) name)
        db-name (if prefix (str prefix "_" db-name) db-name)
        db (clutch/get-database (get-database-url url db-name))]
    (CouchEvictionChannel. db (atom {}))))


















