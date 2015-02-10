(ns lambdaroyal.memory.core.test-tx
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.test-context :refer [meta-model]])
  (:gen-class))


(def context (create-context meta-model))

(dosync
 (let [tx (create-tx context)]
   (insert tx :order :1234 {:type :no})))

(macroexpand (process-constraints :insert 'precommit (:part-order @context) :1234 {:type :no}))

