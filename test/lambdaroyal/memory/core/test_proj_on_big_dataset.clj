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
    (let [tx (create-tx ctx)
          types (list "alpha" "beta" "gamma" "delta" nil nil)]
      (do
        (doseq [p (range 50000)]
          (dosync
           (insert tx :article p {:client (rand-int 100) :type (rand-nth types)})))
        (let [articles (select tx :article)]
          (doseq [p (range 5000)]
            (dosync
             (insert tx :stock p {:article (first (rand-nth articles))}))))))))

(defn- type-decorator [tx [k {article :article}]]
  (let [article (select-first tx :article article)]
    (if-let [type (-> article last :type)]
      [type true]
      [nil false])))

(let [ctx (create-context meta-model)
       tx (create-tx ctx)
       insert (timed @(insert-future ctx))
]
  (println (format "insert took (ms) %s and resulted in %s articles and %s stocks" (first insert) (count (select tx :article)) (count (select tx :stock))))
  (let [projection (timed 
                    (count (proj tx (filter-xs :article (take 5000 (select tx :article))) (>>> :stock :verbose true :parallel true))))]
    (println (format "projection found %s records and took (ms) %s" (last projection) (first projection)))
    (fact "max time for insert" (first insert) => (roughly 0 30000))
    (fact "correct number of articles " (count (select tx :article)) => 50000)
    (fact "max time for projection" (first projection) => (roughly 0 40))

    (append-to-timeseries "proj_on_big_data_set" (apply str (interpose ";" [(first insert) (first projection)]))))
  (with-calculated-field-lambdas {:stock {:type (partial type-decorator tx)}}
    (let [articles (take 5000 (select tx :article))
          projection (timed 
                      (count (proj tx (filter-xs :article articles) (>> :stock (fn [stock] (= "alpha" (-> stock last :type))) :verbose true :parallel true))))
          ;; must return the same as if filtering first the articles
          projection' (timed
                       (count (proj tx (filter-xs :article (filter #(= "alpha" (-> % last :type)) articles)) (>>> :stock :verbose true :parallel true))))]
      (fact "we shoud find some articles at all with type alpha" (> (last projection') 0) => true)
      (fact "we should find the same number of articles when filtering on the calculated attribute" (last projection) => (last projection')))))











