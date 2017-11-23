(ns ^{:doc "An Eviction Channel that uses a Couch DB server to load persistent data and to backup all transaction to. The couch db server
is supposed to run on http://localhost:5984 or as per JVM System Parameter -Dcouchdb.url or individually configured per eviction channel instance"} 
    lambdaroyal.memory.eviction.couchdb
  (:require [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [com.ashafa.clutch :as clutch]
            [com.ashafa.clutch.utils :as utils]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [clojure.set :refer [union]])
  (:use com.ashafa.clutch.http-client)
  (:import [java.net ConnectException]))

(defn- check-couchdb-connection [url]
  (let [_ (log/info (format "try to access couchdb server using url %s" url))
        e (format "Cannot access Couch DB server on %s. Did you start it (probably by sudo /usr/local/etc/init.d/couchdb start" url)]
    (try
      (if-not 
          (= "Welcome" (:couchdb (clutch/couchdb-info url)))
        (throw (IllegalArgumentException. e)))
      (catch ConnectException ex 
        (throw (IllegalStateException. e ex))))))

(defn get-database-url [url db-name]
  (utils/url (utils/url url) db-name))

(defn get-database-url-by-channel [channel coll-name]
  (let [db-name (if (keyword? coll-name) (name coll-name) name)
        db-name (if (.prefix channel) (str (.prefix channel) "_" db-name) db-name)]
    (get-database-url (.url channel) db-name)))

(defn- get-database [channel coll-name]
  (let [db-name (if (keyword? coll-name) (name coll-name) name)
        db-name (if (.prefix channel) (str (.prefix channel) "_" db-name) db-name)]
    (clutch/get-database (get-database-url (.url channel) db-name))))


(defn- is-update-conflict [e]
  (and (instance? clojure.lang.ExceptionInfo e) 
       (if-let [body (-> e ex-data :body)]
         (boolean (re-find #"update conflict" body)))))

(defn- put-document 
  "inserts a document to the couchdb instance referenced by the CouchEvictionChannel [channel]. if a version conflict occurs then this function trys"
  [channel coll-name unique-key user-value]
  (let [rev-key [coll-name unique-key]
        rev (get @(:revs channel) rev-key)
        doc (assoc user-value :_id (str unique-key) :unique-key unique-key)
        doc (if rev (assoc doc :_rev rev) doc)
        put (fn [doc] (try
                        (let [result (clutch/put-document (get-database channel coll-name) doc)]
                          (do 
                            (swap! (.revs channel) assoc rev-key (:_rev result))
                            true))
                        (catch Exception e
                          (if (is-update-conflict e) 
                            false
                            (throw e)))))]
    (try
      (loop [doc doc]
        (if-not (put doc)
          (let [existing (clutch/get-document (get-database channel coll-name) (str unique-key))
                doc (assoc doc :_rev (:_rev existing))]
            (do
              (log/warn (format "update conflict on %s. try again." doc))
              (recur doc)))))
      (catch Exception e
        (log/fatal (format "failed to put document %s to couchdb. " doc) e)))))

(defn- delete-document 
  "deletes a document from the couchdb"
  [channel coll-name unique-key]
  (let [delete (fn [document]
                 (try
                   (do
                     (clutch/delete-document (get-database channel coll-name) document)
                     (swap! (.revs channel) dissoc [coll-name unique-key])
                     true)
                   (catch Exception e
                     (if (is-update-conflict e) 
                       false
                       (throw e)))))]
    (try
      (loop [existing (clutch/get-document (get-database channel coll-name) (str unique-key))]
        (if-not (delete existing)
          (recur (clutch/delete-document (get-database channel coll-name) existing))))
      (catch Exception e
        (log/fatal (format "failed to delete document collection: %s id: %s from couchdb. " coll-name unique-key))))))

(defn- delete-coll
  "deletes a document db from the couchdb"
  [channel coll-name]
  (try
    (do
      (clutch/delete-database
       (get-database channel coll-name)))
    (catch Exception e
      (log/fatal (format "failed to delete db %s from couchdb. " coll-name)))))

(defrecord CouchEvictionChannel [url prefix revs ^clojure.lang.Atom started]
  evict/EvictionChannel
  (start [this ctx colls]
    (future
      ;;order them by referential integrity constraints
      (let [colls (if (> (count colls) 1) 
                    (dependency-model-ordered colls)
                    (map :name colls))]
        (do
          (log/info (format "collection order %s" (apply str (interpose " -> " colls))))
          (log-info-timed 
           "read-in collections"
           (doall 
            (map
             #(let [db (get-database this %)
                    docs (clutch/all-documents db)
                    tx (create-tx ctx :force true)]
                (doseq [doc docs]
                  (let [{:keys [id]} doc
                        existing (clutch/get-document (get-database this %) id) 
                        user-scope-tuple (dosync
                                          (insert tx % (-> existing :unique-key) existing))]
                    (swap! (.revs this) assoc [% (first user-scope-tuple)] (:_rev existing))))
                (log/info (format "collection %s contains %d documents" % (count docs))))
             colls)))
          (reset! (.started this) true)))))
  (started? [this] @(.started this))
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (if @(.started this)
      (put-document this coll-name unique-key user-value)))
  (update [this coll-name unique-key old-user-value new-user-value]
    (if @(.started this)
      (put-document this coll-name unique-key new-user-value)))
  (delete [this coll-name unique-key old-user-value]
    (if @(.started this) (delete-document this coll-name unique-key)))
  (delete-coll [this coll-name]
    (if @(.started this) (delete-coll this coll-name))))

(defn create 
  "provide custom url by calling this function with varargs :url \"https://username:password@account.cloudant.com\""
  [& args]
  (let [args (apply hash-map args)
        {:keys [url prefix] :or {url (or (System/getenv "couchdb.url") "http://localhost:5984")}} args
        _ (check-couchdb-connection url)]
    (CouchEvictionChannel. url prefix (atom {}) (atom false))))

(defn schedule-compaction [eviction-channel ctx]
  (let [next-midnight (fn []
                        (let [next-midnight (.plusDays (.atStartOfDay (java.time.LocalDate/now)) 1)
                              instant (.toInstant (.atZone next-midnight (java.time.ZoneId/systemDefault)))]
                          (java.util.Date/from instant)))
        compaction-fn (fn [coll]
                        (let [coll-name (:name coll)
                              url (str (get-database-url-by-channel eviction-channel coll-name) "/_compact")]
                          (log-info-timed
                           (format "compact collection %s url %s" coll-name url)
                           ;; Send form params as a json encoded body (POST or PUT)
                           (client/post url {:form-params {} :content-type :json}))))]
    (future
      (loop [next (System/currentTimeMillis)]
        (if (.started? eviction-channel)
          (do
            (Thread/sleep 1000)
            (recur
             (if (<= next (System/currentTimeMillis))
               (do
                 (try
                   (doseq [coll (vals @ctx)]
                     (log/info (compaction-fn coll)))
                   (catch Throwable t (log/error t)))
                 (let [next (next-midnight)
                       _ (log/info (format "schedule next compaction for %s" next))]
                   (.getTime next)))
               next))))))))
















