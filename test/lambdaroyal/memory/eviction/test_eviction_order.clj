(ns lambdaroyal.memory.eviction.test-eviction-order
  (:require [midje.sweet :refer :all]
           [lambdaroyal.memory.eviction.core :as evict]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.tx :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))


(def meta-model
  {
   :order
   {:unique true :indexes [] :evictor (evict/create-SysoutEvictionChannel) :evictor-delay 100}})

(try
  (facts "creating eviction scheme"
    (let [ctx (create-context meta-model)
          tx (create-tx ctx)]
      (try
        (do
          (dosync 
           (insert tx :order 1 {:type :gaga :receiver :foo}))
          (Thread/sleep 200)
          

          (fact "try frauting the unique constraint after valid insert in one tx must throw exception" 
            (dosync 
             (insert tx :order 2 {:type :gaga :receiver :foo})
             (insert tx :order 1 {:type :gaga :receiver :foo})) => (throws ConstraintException))
          (Thread/sleep 200)
          
          
          (dosync 
           (alter-document tx :order (select-first tx :order 1) assoc :receiver :boo)
           (delete tx :order 1)
           (insert tx :order 1 {:type :gaga :receiver :foo})
           (delete tx :order 1))
           
          (Thread/sleep 200)
          

          
          (Thread/sleep 400)
          )
        (finally 
          (do
            (.stop (-> @ctx :order :evictor))
            (-> @ctx :order :evictor :consumer deref)))))))










