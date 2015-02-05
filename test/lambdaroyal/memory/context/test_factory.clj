(ns lambdaroyal.memory.context.test-factory
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.context.factory :refer :all])
  (:gen-class))

(def meta-model
  {
   :order
   {:unique true :constraints [] :indexes []}
   :part-order
   {:unique true :constraints [] :indexes []}})

(fact "creating a context from a meta-model"
  (create-context meta-model) => truthy)
