(ns lambdaroyal.memory.core.test-context
  (:require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.context :refer :all])
  (:import [lambdaroyal.memory.core.tx Constraint ReferrerIntegrityConstraint ReferencedIntegrityConstraint AttributeIndex]))

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
              {:name :client-no :unique false :attributes [:client :no]}
              {:name :bool-index :unique false :attributes [:bool]}
              {:name :number-index :unique false :attributes [:number]}]}
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

(def meta-model-with-ric-and-selfreference
  {:type
   {:indexes []}
   :order
   {:indexes []}
   :part-order
   {:indexes [] :foreign-key-constraints [
                                          {:name :type :foreign-coll :type :foreign-key :type}
                                          {:name :order :foreign-coll :order :foreign-key :order}
                                          {:name :parent :foreign-coll :part-order :foreign-key :parent}]}
   :line-item
   {:indexes [] :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]}})

(def meta-model-with-ric'
  {:type
   {:indexes []}
   :order
   {:indexes []}
   :part-order
   {:indexes [] :foreign-key-constraints [
                                          {:name :type :foreign-coll :type :foreign-key :type}
                                          {:name :order :foreign-coll :order :foreign-key :order}]}
   :line-item
   {:indexes [] :foreign-key-constraints [{:name :part-order1 :foreign-coll :part-order :foreign-key :part-order-original}
                                          {:name :part-order2 :foreign-coll :part-order :foreign-key :part-order-old}]}})

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
         (let [constraint-name (referrer-constraint-name :order :order :order)
               ric (get (-> @ctx :part-order :constraints deref) constraint-name)]
           (fact "RIC reveals" ric => truthy)
           (fact "RIC name is correct" (.name ric) => constraint-name)
           (fact "RIC target coll is correct" (.foreign-coll ric) => :order)
           (fact "RIC key is correct" (.foreign-key ric) => :order))
         (let [constraint-name (referrer-constraint-name :type :type :type)
               ric (get (-> @ctx :part-order :constraints deref) constraint-name)]
           (fact "RIC reveals" ric => truthy)
           (fact "RIC name is correct" (.name ric) => constraint-name)
           (fact "RIC target coll is correct" (.foreign-coll ric) => :type)
           (fact "RIC key is correct" (.foreign-key ric) => :type))
         (let [constraint-name (referenced-constraint-name :part-order :order :order)
               ric (get (-> @ctx :order :constraints deref) constraint-name)]
           (fact "RIC as referenced integrity constraint is given on the referenced column" (type ric) => lambdaroyal.memory.core.tx.ReferencedIntegrityConstraint)
           (fact "Referenced Integrity Constraint is pointing to the proper source collection" (.referencing-coll ric) => :part-order)
           (fact "Referenced Integrity Constraint is pointing to the proper" (.referencing-key ric) => :order))))

(facts "facts about adding and removing constraints (RICs) at runtime"
       (let [ctx (create-context meta-model)]
         (add-ric ctx {:name :part-order->order :coll :part-order :foreign-coll :order :foreign-key :order})
         (add-ric ctx {:name :part-order->order#2 :coll :part-order :foreign-coll :order :foreign-key :order2})
         (let [constraint-name (referrer-constraint-name :order :order :part-order->order)
               ric (get (-> @ctx :part-order :constraints deref) constraint-name)]
           (fact "RIC reveals" ric => truthy)
           (fact "RIC name is correct" (.name ric) => constraint-name)
           (fact "RIC target coll is correct" (.foreign-coll ric) => :order)
           (fact "RIC key is correct" (.foreign-key ric) => :order))

         (comment (fact "check RICs after add-ric"
                        (map (fn [x] {:from (.referencing-coll x) :via (.referencing-key x) :name (.name x)}) 
                             (filter
                              #(instance? ReferencedIntegrityConstraint %) 
                              (map last (-> @ctx :order :constraints deref)))) => falsey))
         (fact "proper reference must be present" (empty? (filter #(and 
                                                                    (= (.referencing-coll %) :part-order)
                                                                    (= (.referencing-key %) :order)
                                                                    )
                                                                  (filter
                                                                   #(instance? ReferencedIntegrityConstraint %) (map last (-> @ctx :order :constraints deref))))) => false)
         (fact "proper reference from order2 must be present" (empty? (filter #(and 
                                                                                (= (.referencing-coll %) :part-order)
                                                                                (= (.referencing-key %) :order2)
                                                                                )
                                                                              (filter
                                                                               #(instance? ReferencedIntegrityConstraint %) (map last (-> @ctx :order :constraints deref))))) => false)
         (fact "removing the RIC works"
               (do
                 (remove-ric ctx :part-order :order :order)
                 (-> @ctx :part-order :constraints deref :part-order->order)) => nil)
         (fact "no more referencing constraints afterwards" (empty? 
                                                             (filter #(and 
                                                                       (= (.referencing-coll %) :part-order)
                                                                       (= (.referencing-key %) :order))
                                                                     (filter
                                                                      #(instance? ReferencedIntegrityConstraint %) (map last (-> @ctx :order :constraints deref))))) => true)
         (fact "proper reference from order2 must be still present" (empty? (filter #(and 
                                                                                      (= (.referencing-coll %) :part-order)
                                                                                      (= (.referencing-key %) :order2)
                                                                                      )
                                                                                    (filter
                                                                                     #(instance? ReferencedIntegrityConstraint %) (map last (-> @ctx :order :constraints deref))))) => false)
         (fact "all other dynamically added rics must still be present"
               (get (-> @ctx :part-order :constraints deref)
                    (referrer-constraint-name :order :order2 :part-order->order#2)
                    ) => truthy)))


(facts "testing the y-combinator to used build the dependency model"
       (fact "reveal proper collection order using the convenience function"
             (dependency-model-ordered (-> (create-context meta-model-with-ric) deref vals)) => '(:type :order :part-order :line-item))
       (fact "reveal proper collection order using a model with just one collection"
             (dependency-model-ordered (-> (create-context {:sys_state {:indexes []}}) deref vals)) => '(:sys_state)))

(facts "testing the y-combinator to used build the dependency model - with self references"
       (fact "reveal proper collection order using the convenience function"
             (dependency-model-ordered (-> (create-context meta-model-with-ric-and-selfreference) deref vals)) => '(:type :order :part-order :line-item))) 
