(ns lambdaroyal.memory.core.test-context
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.context :refer :all])
  (import [lambdaroyal.memory.core.tx Constraint]))

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
   {:unique true :indexes [{:name :client :unique false :attributes [:client]}
                           {:name :client-no :unique false :attributes [:client :no]}]}
   :part-order
   {:unique true :indexes []}
   :interaction
   {:indexes [{:attributes [:keyword]}]}})

(def meta-model-with-ric
  {:type
   {:unique true :indexes []}
   :order
   {:unique true :indexes []}
   :part-order
   {:unique true :indexes [] :foreign-key-constraints [
                                                       {:name :type :foreign-coll :type :foreign-key :type}
                                                       {:name :order :foreign-coll :order :foreign-key :order}]}})

(facts "facts about the created context with indexes"
  (fact "can create" (create-context meta-model-with-indexes) => truthy))

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

(facts "facts abount context creation with referential integrity constraints (RIC)"
  (let [rics (map (fn [[coll constraint]] [coll constraint]) (referential-integrity-constraint-factory meta-model-with-ric))
        ctx (create-context meta-model-with-ric)]
    (fact "creating a context from a meta-model" ctx -> truthy)
    (let [ric (-> @ctx :part-order :constraints deref :order)]
      (fact "RIC reveals" ric => truthy)
      (fact "RIC name is correct" (.name ric) => :order)
      (fact "RIC target coll is correct" (.foreign-coll ric) => :order)
      (fact "RIC key is correct" (.foreign-key ric) => :order))
    (let [ric (-> @ctx :part-order :constraints deref :type)]
      (fact "RIC reveals" ric => truthy)
      (fact "RIC name is correct" (.name ric) => :type)
      (fact "RIC target coll is correct" (.foreign-coll ric) => :type)
      (fact "RIC key is correct" (.foreign-key ric) => :type))
    (let [ric (-> @ctx :order :constraints deref :order)]
      (fact "RIC is not duplicated falsey" ric => falsey))))















