(ns 
    ^{:doc "Performance Unittests for lambdaroyal memory. result eviction to influxdb."
      :author "christian.meichsner@live.com"}
  lambdaroyal.memory.core.test-proj-on-big-dataset
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.abstraction.search :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [clojure.core.async :refer [>! alts!! timeout chan go]])
  (:import [java.text SimpleDateFormat]))

;;we try to be idempotent, so we don't use mutable models from other workspaces here

(def meta-model
  {
   :article
   {:indexes [{:name :client :unique false :attributes [:client]}
                           {:name :client-no :unique false :attributes [:client :no]}]}

   :stock
   {:indexes [] :foreign-key-constraints [{:name :article :foreign-coll :article :foreign-key :article}]}})

(defn- insert-future [ctx]
  "spawns a thread that starts 100 consecutive transactions, each transaction consists of 10 inserts"
  (future
    (let [tx (create-tx ctx)]
      (do
        (doseq [p (range 50000)]
          (dosync
           (insert tx :article p {:client (rand-int 100)})))
        (let [articles (select tx :article)]
          (doseq [p (range 50)]
            (dosync
             (insert tx :stock p {:article (first (rand-nth articles))}))))))))

+(let [ctx (create-context meta-model)
       tx (create-tx ctx)
       bulk (timed @(insert-future ctx))
       _ (println "test took (ms) " (first bulk))]
   (fact "max time for insert" (first bulk) => (roughly 0 15000))
   (fact "correct number of articles " (count (select tx :article)) => 50000)
   (println :start-search)
   (fact "timed search took less than 150 msec" 
         (first 
          (timed 
           (proj tx (filter-xs :article (take 5000 (select tx :article))) (>>> :stock :verbose true :parallel true)))) => (roughly 0 15)))
