(ns lambdaroyal.memory.core.test-context
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.context :refer :all])
  (import [lambdaroyal.memory.core.tx Constraint ReferrerIntegrityConstraint]))

(def meta-model
  {
   :order
   {:indexes []}
   :part-order
   {:indexes []}
   :interaction
   {:indexes []}})

(def meta-model-with-indexes
  {
   :order
   {:indexes [{:name :client :unique false :attributes [:client]}
                           {:name :client-no :unique false :attributes [:client :no]}]}
   :part-order
   {:indexes []}
   :interaction
   {:indexes [{:attributes [:keyword]}]}})

(def meta-model-with-ric
  {:type
   {:indexes []}
   :order
   {:indexes []}
   :part-order
   {:indexes [] :foreign-key-constraints [
                                                       {:name :type :foreign-coll :type :foreign-key :type}
                                                       {:name :order :foreign-coll :order :foreign-key :order}]}
   :line-item
   {:indexes [] :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]}})

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
      (-> @ctx :interaction) => truthy)))

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

(facts "facts about adding constraints (RICs) at runtime"
  (let [ctx (create-context meta-model)]
    (add-ric ctx {:name :part-order->order :coll :part-order :foreign-coll :order :foreign-key :order})
    (let [ric (-> @ctx :part-order :constraints deref :part-order->order)]
      (fact "RIC reveals" ric => truthy)
      (fact "RIC name is correct" (.name ric) => :part-order->order)
      (fact "RIC target coll is correct" (.foreign-coll ric) => :order)
      (fact "RIC key is correct" (.foreign-key ric) => :order))
    (fact "adding a second time does not harm" 
      (add-ric ctx {:name :part-order->order :coll :part-order :foreign-coll :order :foreign-key :order}) => nil)))


(facts "testing the y-combinator to used build the dependency model"
  (let [model {:order [:order-type :client] :part-order [:order :article] :article [:client] :client [] :order-type []}]
    (fact "reveal proper dependency" (apply concat (take-while not-empty (map last (rest (iterate dependency-order [model]))))) => #(or                                                                                             (= % '(:client :order-type :article :order :part-order))
(= % '(:client :order-type :order :article :part-order))))
    (fact "reveal proper dependencies on the meta modell"
      (apply concat (take-while not-empty (map last (rest (iterate dependency-order [(dependency-model (-> (create-context meta-model-with-ric) deref vals))]))))) => '(:type :order :part-order :line-item))
    (fact "reveal proper collection order using the concenience function"
      (map :name (dependency-model-ordered (-> (create-context meta-model-with-ric) deref vals))) => '(:type :order :part-order :line-item))
    (fact "reveal proper collection order using a model with just one collection"
      (map :name (dependency-model-ordered (-> (create-context {:sys_state {:indexes []}}) deref vals))) => '(:sys_state))))
