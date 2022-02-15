(ns lambdaroyal.memory.core.test-gtid-increment
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))

(let [ctx (create-context meta-model)
      tx (create-tx ctx)
      old-gtid @gtid
      order (dosync
             (insert tx :order :a {:type "test"}))
      _ (fact "gtid should increase by one"
          (= 1 (- @gtid old-gtid)) => truthy)
      old-gtid @gtid
      _ (dosync
         (alter-document tx :order order assoc :type "test2" ))
      _ (fact "gtid should increase by one"
          (= 1 (- @gtid old-gtid)) => truthy)
      old-gtid @gtid
      _ (dosync
         (delete tx :order (first order)))
      _ (fact "gtid should increase by one"
          (= 1 (- @gtid old-gtid)) => truthy)])
