(ns ^{:doc "An Eviction Channel that uses a Mongo DB server to load persistent data and to backup all transaction to. The couch db server
is supposed to run on http://localhost:5984 or as per JVM System Parameter -Dcouchdb.url or individually configured per eviction channel instance"}
 lambdaroyal.memory.eviction.mongodb
  (:require [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.eviction.wal :as wal]
            [cheshire.core :as json]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clj-http.client :as client]
            [clojure.set :refer [union]])
  (:import [java.net ConnectException]))

(def flush-idx (atom (- (System/currentTimeMillis) (.getTime (.parse (java.text.SimpleDateFormat. "ddMMyyyy") "11092019")))))
(def verbose (atom false))

;; change this to true for testing purposes
(def read-only (atom false))

(def flush-log-format "FL %3s %3s %12d | %30s %-20s %s")
(defn- log-try [method collection id rev]
  (if @verbose
    (let [flush-idx' (swap! flush-idx inc)]
      (log/info (format flush-log-format method "TRY" flush-idx' (name collection) id (or rev "")))
      flush-idx')))

(format "g%-12s" "foo")

(defn- log-result [flush-idx' ok method collection id rev]
  (if @verbose
    (log/info (format flush-log-format method (if ok "OK" "NOK") flush-idx' (name collection) id (or rev "")))))

(defn- check-mongodb-connection [url db]
  (let [_ (log/info (format "try to access mongodb server using url %s" url))
        _ (if (-> read-only deref true?) (log/info "MongoDB evictor works in READ-ONLY MODE. THIS IS NOT THE PRODUCTION MODE"))
        e (format "Cannot access Mongo DB server on %s. Did you start it?" url)]
    (if-not
     (= 1.0 (get (mg/command db {:ping 1}) "ok"))
      (throw (IllegalArgumentException. e)))))

(defn get-database-url [prefix-url username password postfix-url]
  (str prefix-url username ":" password "@" postfix-url))

(defn get-database [db-name conn]
  (mg/get-db (conn :conn) db-name))

(defn get-connection [url]
  (mg/connect-via-uri url))

(defn- put-document [this coll-name unique-key user-value db]
  (if @verbose
    (println :put-document coll-name unique-key user-value))
  (mc/insert db coll-name (assoc user-value :_id unique-key)))

(defn- update-document [this coll-name unique-key user-value db]
  (if @verbose
    (println :update-document coll-name unique-key user-value))
  (let [wal-payload {:fn :update :coll coll-name :id unique-key :val user-value}]
    (mc/update-by-id db coll-name unique-key user-value)))

(defn- delete-document
  [channel coll-name unique-key db]
  (if @verbose
    (println :delete-document coll-name unique-key))
  (mc/remove db coll-name {:_id unique-key}))

(defn- delete-all [channel url db-name coll-name]
  (let [conn (get-connection url)
        db (get-database db-name conn)]
    (mc/remove db coll-name)))

(defn- get-all-documents [db coll]
  (mc/find-maps db coll))

(defrecord MongoEvictionChannel [url db-name db-ctx ^clojure.lang.Atom started ^clojure.lang.Atom stopped]
  evict/EvictionChannel
  (start [this ctx colls]
    (future
      ;;order them by referential integrity constraints
      (let [colls (if (> (count colls) 1)
                    (dependency-model-ordered colls)
                    (map :name colls))
            conn (get-connection url)
            db (get-database db-name conn)])
      (if (-> @db-ctx :wal-queue nil?)
        (let [queue (wal/create-queue "mongodb")
              _ (swap! db-ctx assoc :wal-queue queue)
              _ (wal/start-queue queue #(@(.stopped this))
                                 (fn [payload]
                                   (let [{func "fn" coll "coll" id "id" val "val"} (json/parse-string payload)]
                                     (cond
                                       (= func "insert") (try (do
                                                                (mc/insert db coll (assoc val :_id id))
                                                                {:success true})
                                                              (catch Exception e {:success false :error-msg (str "Caught exception: " (.getMessage e))}))
                                       (= func "update") (try (do (mc/update-by-id db coll id val)
                                                                  {:success true})
                                                              (catch Exception e {:success false :error-msg (str "Caught exception: " (.getMessage e))}))
                                       (= func "delete") (try (do (mc/remove db coll {:_id id})
                                                                  {:success true})
                                                              (catch Exception e {:success false :error-msg (str "Caught exception: " (.getMessage e))}))))))]
          (do
            (swap! db-ctx assoc :conn conn :db db)
            (println (format "collection order %s" (apply str (interpose " -> " colls))))
            (log-info-timed
             "read-in collections"
             (wal/loop-until-ok
              (fn [] (doall
                      (map
                       (fn [coll] (let [docs (get-all-documents (:db @db-ctx) coll)
                                        tx (create-tx ctx :force true)]
                                    (doseq [doc docs]
                                      (let [existing doc
                                            user-scope-tuple (dosync
                                                              (insert-raw tx coll (:_id existing) existing))]))
                                    (println (format "collection %s contains %d documents" coll (count docs)))))
                       colls)))
            ;; This is the condition not ok function
              (fn [] (and (some? (wal/peek-queue (:wal-queue @db-ctx))) @(.stopped this)))
              "Still reading from the queue and persist to MongoDB"
              1000))
            (println :type (-> this .started type))
            (reset! (.started this) true))))))
  (started? [this] @(.started this))
  (stopped? [this]  @(.stopped this))
  (insert [this coll-name unique-key user-value]
    (let [payload (wal/get-wal-payload :insert coll-name unique-key user-value)]
      (wal/insert-into-queue payload (:wal-queue @db-ctx))))
  (stop [this]
        (.close (:wal-queue @db-ctx))
        (mg/disconnect (:conn (:conn @db-ctx)))
        (swap! (.stopped this) not)
        (if @verbose (println :stop "MongoDB evictor-channel stopped")))
  (update [this coll-name unique-key old-user-value new-user-value]
    (let [payload (wal/get-wal-payload :update coll-name unique-key new-user-value)]
      (wal/insert-into-queue payload (:wal-queue @db-ctx))))
  (delete [this coll-name unique-key old-user-value]
    (let [payload (wal/get-wal-payload :delete coll-name unique-key old-user-value)]
      (wal/insert-into-queue payload (:wal-queue @db-ctx))))
  (delete-coll [this coll-name]
    (delete-all this url db-name coll-name))
  evict/EvictionChannelHeartbeat
  (alive? [this] (try
                   (check-mongodb-connection url (:db @db-ctx))
                   (catch Exception e false)))
  evict/EvictionChannelCompaction
  (compaction [this ctx]
    nil))
  

(defn create
  "provide custom url by calling this function with varargs :url \"https://username:password@account.cloudant.com\""
  [& args]
  (let [args (if args (apply hash-map args) {})
        {:keys [url db-name] :or {url (get-database-url (System/getProperty "mongodb_preurl") (System/getProperty "mongodb_username") (System/getProperty "mongodb_password") (System/getProperty "mongodb_posturl")) db-name (System/getProperty "mongodb_dbname")}} args
        conn (get-connection url)
        _ (check-mongodb-connection url (get-database db-name conn))]
    (MongoEvictionChannel. url db-name (atom {}) (or (:started args) (atom false)) (atom false))))

