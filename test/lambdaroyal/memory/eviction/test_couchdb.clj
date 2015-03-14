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
   (doseq [r (range 1)]
     (insert tx :order r {:type :gaga :receiver :foo :run r})))
  (dosync
   (alter-document tx :order (select-first tx :order 1) assoc :type :gigi))
  (dosync
   (delete tx :order 0))
  (.stop (-> @ctx :order :evictor))
  (-> @ctx :order :evictor :consumer deref))

(facts "checking couchdb eviction scheme with insert/update/delete in single tx"
  (dosync
   (doseq [r (range 1)]
     (insert tx :order r {:type :gaga :receiver :foo :run r}))
   (alter-document tx :order (select-first tx :order 0) assoc :type :gigi)
   (delete tx :order 0))
  (.stop (-> @ctx :order :evictor))
  (-> @ctx :order :evictor :consumer deref))



















