(ns lambdaroyal.memory.eviction.test-couchdb
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.eviction.core :as evict]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.eviction.couchdb :as evict-couchdb])
  (import [lambdaroyal.memory.core ConstraintException]))

(def meta-model
  {
   :order
   {:unique true :indexes [] :evictor (evict-couchdb/create :order) :evictor-delay 1000}})

(def ctx (create-context meta-model))
(def tx (create-tx ctx))

(facts "creating eviction scheme"
  (dosync
   (doseq [r (range 1000)]
     (insert tx :order r {:type :gaga :receiver :foo :run r})))
  (.stop (-> @ctx :order :evictor))
  (-> @ctx :order :evictor :consumer deref))



















