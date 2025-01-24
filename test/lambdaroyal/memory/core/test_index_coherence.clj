(ns lambdaroyal.memory.core.test-index-coherence
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))

(defn scan
  "INDEXED SEARCH. Scans a collection :datatype on an atomic index for all records whose attribute value on the :attribute matches the value"
  [tx datatype attribute value]
  {:pre [(keyword? datatype) (keyword? attribute)]}
  (take-while #(= value (get (last %) attribute)) (select tx datatype [attribute] >= [value])))

(facts "Check ordering of records after an indexed query"
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        coll' (-> tx :context deref :order)
        i' (first (applicable-indexes coll' [:client]))
        
        _ (dosync
           (insert tx :order  0 {:client 1})
           (insert tx :order  6 {:client 1})
           (insert tx :order  3 {:client 2})
           (insert tx :order 2 {:client 2})
           (insert tx :order 4 {:client 2})
           (insert tx :order 7 {:client 3})
           (insert tx :order 1 {:client 3}))
        
        select-res  (select tx :order [:client] >= [0] <= [3])
        rselect-res (rselect tx :order [:client] >= [0] <= [3])
        

        record-by-index (fn [id] (let [data (get-data i')]
                                   (let [r (first (filter (fn [[k v]] (= id (first v))) data))]
                                     (first r))))]

    
    (fact "there should be an index" i' => some?)
    (fact "(1) the index contains the relevant information" (record-by-index 6) => [[1] 6])
    (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0 6])

    _ (let [x (select-first tx :order 6)]
        (dosync (alter-document tx :order x assoc :client 2)))
    (fact "(1) the index contains the relevant information" (record-by-index 6) => [[2] 6])
    (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0])
    _ (let [x (select-first tx :order 6)]
        (dosync (alter-document tx :order x assoc :client 1)))
    (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0 6])
    (fact "the index contains the relevant information" (record-by-index 6) => [[1] 6])

    
    _ (let [x (select-first tx :order 6)]
        (println :try-both-things)
        (dosync 
         (record-by-index 6)
         (alter-document tx :order x dissoc :client)
         (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0])
         (record-by-index 6)
         (alter-document tx :order x assoc :client 1)
         (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0 6])))
    (fact "the index contains the relevant information" (record-by-index 6) => [[1] 6])
    (fact "there should be two orders with client 1" (map first (scan tx :order :client 1)) => [0 6])


    (fact "select should be ordered according to index by ascending primary key" (map first select-res) => '(0 6 2 3 4 1 7))
    
    
    
    (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(7 1 4 3 2 6 0 )))

)












