(ns lambdaroyal.memory.core.test-context
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.context :refer :all])
  (import [lambdaroyal.memory.core.tx Constraint])
  (:gen-class))

(def meta-model
  {
   :order
   {:unique true :indexes []}
   :part-order
   {:unique true :indexes []}
   :interaction
   {:indexes []}})

(def meta-model-with-indexes
  {
   :order
   {:unique true :indexes [{:name :client :unique false :attributes [[:client]]}]}
   :part-order
   {:unique true :indexes []}
   :interaction
   {:indexes [{:attribues [[:keyword]]}]}})


(facts "facts about the created context"
  (let [ctx (create-context meta-model)]
    (fact "creating a context from a meta-model"
      ctx => truthy)
    (fact "context must contain a collection :order"
      (-> @ctx :order) => truthy)
    (fact "context must contain a collection :part-order"
      (-> @ctx :part-order) => truthy)
    (fact "context must contain a collection :interaction"
      (-> @ctx :interaction) => truthy)
    (fact "collections order as well as part-order must contain unique constraint"
      (set 
       (map 
        first 
        (filter 
         (fn [[k v]] (if-let [constraint (-> v :constraints deref :unique-key)]
                       (instance? Constraint constraint)))
         @ctx))) => #{:order :part-order})))














