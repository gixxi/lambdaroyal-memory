(ns 
    ^{:doc "tests calculating attribute values on the fly"
      :author "christian.meichsner@live.com"}
  lambdaroyal.memory.core.test-calculated-fields
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
          types (list "alpha" "beta" "gamma" "delta" nil)]
      (doseq [p (range 100)]
        (dosync
         (let [article (insert tx :article p {:client (rand-int 100) :type (rand-nth types)})]
           (insert tx :stock p {:type' (-> article last :type) :article (first article) :amount (inc (rand-int 10))})))))))

(defn- type-decorator [tx [k {article :article}]]
  (let [article (select-first tx :article article)]
    (if-let [type (-> article last :type)]
      [type true]
      [nil false])))

(defn- is-beta-decorator [tx [k {article :article}]]
  (let [article (select-first tx :article article)]
    (if (= "beta" (-> article last :type))
      [true true]
      [nil false])))

(let [ctx (create-context meta-model)
       tx (create-tx ctx)
       insert (timed @(insert-future ctx))
]
  (println (format "insert took (ms) %s and resulted in %s articles and %s stocks" (first insert) (count (select tx :article)) (count (select tx :stock))))
  (with-calculated-field-lambdas {:stock {:type (partial type-decorator tx)
                                          :is-beta (partial is-beta-decorator tx)}}
    (let [stock (first (select tx :stock))]
      (println :stock stock))
    (let [alphas (filter #(= "alpha" (-> % last :type')) (select tx :stock))
          betas (filter #(= "beta" (-> % last :type')) (select tx :stock))
          nils (filter #(-> % last :type' nil?) (select tx :stock))]
      (println :beta (first betas))
      (fact "there are some alphas" (empty? alphas) => false)
      (fact "there are some betas" (empty? betas) => false)
      (fact "there are some nils" (empty? nils) => false)
      (fact "every stock stemming from alpha article denotes the proper type and :vlicCalculated attribute"
            (every? (fn [[k {:keys [type vlicCalculated]}]]
                      (and (= "alpha" type)
                           (= (list :type) vlicCalculated))) alphas) => true)
      (fact "every stock stemming from beta article denotes the proper type and :vlicCalculated attribute"
            (every? (fn [[k {:keys [type vlicCalculated is-beta]}]]
                      (and (= "beta" type)
                           (true? is-beta)
                           (= (list :is-beta :type) vlicCalculated))) betas) => true)
      (fact "every stock stemming from article with nil"
            (every? (fn [[k {:keys [type vlicCalculated]}]]
                      (and (nil? type)
                           (nil? vlicCalculated))) nils) => true))))











