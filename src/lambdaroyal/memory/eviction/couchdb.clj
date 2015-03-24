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
           [clojure.tools.logging :as log]
           [clojure.set :refer [union]]))

(defn- check-couchdb-connection [url]
  (if-not 
      (= "Welcome" (:couchdb (clutch/couchdb-info url)))
    (throw (IllegalArgumentException. (format "Cannot access Couch DB server on %s" url)))))

(defn get-database-url [url db-name]
  (utils/url (utils/url url) db-name))

(defn- get-database [channel coll-name]
  (let [db-name (if (keyword? coll-name) (name coll-name) name)
        db-name (if (.prefix channel) (str (.prefix channel) "_" db-name) db-name)]
    (clutch/get-database (get-database-url (.url channel) db-name))))


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
        put (fn [doc] (let [result (clutch/put-document (get-database channel coll-name) doc)]
                        (swap! (.revs channel) assoc rev-key (:_rev result))))]
    (try
      (put doc)
      (catch Exception e 
        ;;retry
        (let [_ (if (is-update-conflict e)
                  (log/warn (format "update conflict on %s. try again." doc)))
              existing (clutch/get-document (get-database channel coll-name) (str unique-key))
              doc (assoc doc :_rev (:_rev existing))]
          (try
            (put doc)
            (catch Exception e
              (log/fatal (format "failed to put document %s to couchdb during retry. stop eviction channel." doc))
              (.stop channel))))))))

(defn- delete-document 
  "deletes a document from the couchdb"
  [channel coll-name unique-key]
  (if-let [existing (clutch/get-document (get-database channel coll-name) (str unique-key))]
    (try
      (do
        (clutch/delete-document (get-database channel coll-name) existing)
        (swap! (.revs channel) dissoc [coll-name unique-key]))
      (catch Exception e
        (log/fatal (format "failed to delete document %s from couchdb. stop eviction channel." existing))
        (.stop channel)))))

(defrecord CouchEvictionChannel [url prefix revs ^clojure.lang.Atom started]
  evict/EvictionChannel
  (start [this ctx colls]
    (future
      ;;order them by referential integrity constraints
      (let [colls (dependency-model-ordered colls)]
        (do
          (doseq [r colls]
            (log/debug (format "read-in collection %s" (:name r))))
          (log-info-timed 
           "read-in collections"
           (doall 
            (map
             #(let [db (get-database this (:name %))
                    docs (clutch/all-documents db)
                    tx (create-tx ctx :force true)]
                (do
                  (doseq [doc docs]
                    (let [{:keys [id]} doc
                          existing (clutch/get-document (get-database this (:name %)) id) 
                          user-scope-tuple (dosync
                                            (insert tx (:name %) (-> existing :unique-key first) existing))]
                      (swap! (.revs this) assoc [(:name %) (first user-scope-tuple)] (:_rev existing))))
                  (log/info (format "collection %s contains %d documents" (:name %) (count docs)))))
             colls)))
          (swap! (.started this) not)))))
  (started? [this] @(.started this))
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (if @(.started this)
      (put-document this coll-name unique-key user-value)))
  (update [this coll-name unique-key old-user-value new-user-value]
    (if @(.started this)
      (put-document this coll-name unique-key new-user-value)))
  (delete [this coll-name unique-key]
    (if @(.started this) (delete-document this coll-name unique-key))))

(defn create 
  "provide custom url by calling this function with varargs :url \"https://username:password@account.cloudant.com\""
  [& args]
  (let [args (apply hash-map args)
        {:keys [url prefix] :or {url (or (System/getenv "couchdb.url") "http://localhost:5984")}} args
        _ (check-couchdb-connection url)]
    (CouchEvictionChannel. url prefix (atom {}) (atom false))))


















