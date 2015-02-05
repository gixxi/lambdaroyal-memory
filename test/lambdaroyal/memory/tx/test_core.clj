(ns lambdaroyal.memory.tx.test-core
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.tx.core :refer :all]
           [lambdaroyal.memory.context.factory :refer :all]
           [lambdaroyal.memory.context.test-factory :refer [meta-model]])
  (:gen-class))

(def context (create-context meta-model))

(dosync
 (let [tx (create-tx context)]
   (insert tx :order :1234 {:type :normal})))











