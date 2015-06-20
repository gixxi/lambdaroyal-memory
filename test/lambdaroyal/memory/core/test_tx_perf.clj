(ns 
    ^{:doc "Performance Unittests for lambdaroyal memory. result eviction to influxdb."
      :author "christian.meichsner@live.com"}
  lambdaroyal.memory.core.test-tx-perf
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [clojure.core.async :refer [>! alts!! timeout chan go]]
            
            )
  (:import [java.text SimpleDateFormat]))

;;we try to be idempotent, so we don't use mutable models from other workspaces here

(def ^:const threads 10)

(def meta-model
  {:type
   {:indexes []}
   :order
   {:indexes [{:name :client :unique false :attributes [:client]}
                           {:name :client-no :unique false :attributes [:client :no]}]}
   :part-order
   {:indexes [] :foreign-key-constraints [
                                                       {:name :type :foreign-coll :type :foreign-key :type}
                                                       {:name :order :foreign-coll :order :foreign-key :order}]}
   :line-item
   {:indexes [] :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]}})

(defn- insert-future [ctx prefix]
  "spawns a thread that starts 100 consecutive transactions, each transaction consists of 10 inserts"
  (future
    (let [tx (create-tx ctx)]
      (doseq [p (partition 10 (range 1000))]
        (dosync
         (doseq [i p]
           (let [idx (+ (* 1000 prefix) i)]
             (insert tx :order idx {:client (rand-int 100)})
             (insert tx :part-order (format "%d:%s" idx "A") {:order idx :type 1 :sort :first})
             (insert tx :part-order (format "%d:%s" idx "B") {:order idx :type 1 :sort :first}))))))
    prefix))

(defn append-to-timeseries [name & values]
  (if (not= "false" (System/getenv "lambdaroyal.memory.traceteststats.disable"))
    (let [dir (or (System/getenv "lambdaroyal.memory.traceteststats.dir") "test/stats/")
          filename (str dir name ".dat")
          format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
      (with-open [w (clojure.java.io/writer filename :append true)]
        (.write w (apply str (.format format (new java.util.Date)) \; values))
        (.write w "\n")))))

(facts "check timing constraints inserting 10000 orders and 20000 partorders with 10 futures, each future executing 1000 transaction a 10+20 inserts"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync (insert tx :type 1 {}))
        bulk (timed
              (reduce (fn [acc i] (+ acc @i)) 0 (pmap #(insert-future ctx %) (range threads))))
        _ (println "test took (ms) " (first bulk))
        _ (append-to-timeseries "30000insertsBy10Threads" (first bulk))]
    (fact "all elements inserted" (-> ctx deref :order :data deref count) => (* 1000 threads))
    (fact "all threads finished" (float (last bulk)) => (Math/floor (/ (* threads (dec threads)) 2)))
    (fact "max time for 10000+20000 parallel inserts" (first bulk) => (roughly 0 15000))))

(defn wait [ms f & args]
  (let [c (chan)]
    (go (>! c (apply f args)))
    (first (alts!! [c (timeout ms)]))))




















