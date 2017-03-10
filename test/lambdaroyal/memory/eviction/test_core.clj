(ns lambdaroyal.memory.eviction.test-core
  (:require [midje.sweet :refer :all]
           [lambdaroyal.memory.eviction.core :as evict]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.tx :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))


(defrecord CounterEvictionChannel [insert-count update-count delete-count started]
  evict/EvictionChannel
  (start [this ctx colls] (future nil))
  (started? [this] @(.started this))
  (stop [this] nil)
  (stopped? [this] nil)
  (insert [this coll-name unique-key user-value]
    (do
      (println :insert)
      (swap! insert-count inc)))
  (update [this coll-name unique-key old-user-value new-user-value]
    (do
      (println :update)
      (swap! update-count inc)))
  (delete [this coll-name unique-key old-user-value]
    (do
      (println :delete)
      (swap! delete-count inc)))
  (delete-coll [this coll-name] nil))

(def insert-count (atom 0))
(def update-count (atom 0))
(def delete-count (atom 0))
(def counter-evictor (CounterEvictionChannel. insert-count update-count delete-count (atom true)))

(def meta-model
  {
   :order
   {:unique true :indexes [] :evictor counter-evictor :evictor-delay 100}})

(try
  (facts "creating eviction scheme"
    (let [ctx (create-context meta-model)
          tx (create-tx ctx)]
      (try
        (do
          (fact "start a zero counter" @insert-count => 0)
          (fact "start a zero counter" @update-count => 0)
          (fact "start a zero counter" @delete-count => 0)
          (dosync 
           (insert tx :order 1 {:type :gaga :receiver :foo}))
          (Thread/sleep 200)
          (fact "inc insert counter" @insert-count => 1)

          (fact "try frauting the unique constraint after valid insert in one tx must throw exception" 
            (dosync 
             (insert tx :order 2 {:type :gaga :receiver :foo})
             (insert tx :order 1 {:type :gaga :receiver :foo})) => (throws ConstraintException))
          (Thread/sleep 200)
          (fact "failed tx does not impose eviction" @insert-count => 1)
          
          (dosync 
           (alter-document tx :order (select-first tx :order 1) assoc :receiver :boo))
          (Thread/sleep 200)
          (fact "insert counter remains after update" @insert-count => 1)
          (fact "delete counter remains after update" @delete-count => 0)
          (fact "update counter inc" @update-count => 1)

          (dosync 
           (alter-document tx :order (select-first tx :order 1) assoc :receiver :boo2 :client :lambda)
           (alter-document tx :order (select-first tx :order 1) assoc :receiver :boo3))
          (Thread/sleep 400)
          (fact "update counter inc - pay attention: multiple updates within one tx result in one eviction only" @update-count => 2)

          
          (dosync 
           (delete tx :order 1))
          (Thread/sleep 400)
          (fact "delete counter inc" @delete-count => 1))
        (finally 
          (do
            (.stop (-> @ctx :order :evictor))
            (-> @ctx :order :evictor :consumer deref)))))))










