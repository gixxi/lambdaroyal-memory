(ns lambdaroyal.memory.core.test-tx-indexing
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))


(facts "Check ordering of records after an indexed query"
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  6 {:client 1})
           (insert tx :order  3 {:client 2})
           (insert tx :order 2 {:client 2})
           (insert tx :order 4 {:client 2})
           (insert tx :order 7 {:client 3})
           (insert tx :order 1 {:client 3}))
        
        select-res  (select tx :order [:client] >= [0] <= [3])
        rselect-res (rselect tx :order [:client] >= [0] <= [3])]
    (fact "select should be ordered according to index by ascending primary key" (map first select-res) => '(6 2 3 4 1 7))
    
    
    
    (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(7 1 4 3 2 6)))


  ;; Testing for string index attributes keys
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  6 {:client "1"})
           (insert tx :order  5 {:client "2"})
           (insert tx :order 1 {:client "2"})
           (insert tx :order 2 {:client "2"})
           (insert tx :order 3 {:client "3"})
           (insert tx :order 4 {:client "3"}))
        
        select-res  (select tx :order [:client] >= ["1"] <= ["3"])
        
        rselect-res (rselect tx :order [:client] >= ["1"] <= ["3"])
        
        ]
    (fact "select should be ordered according to index by ascending primary key" (map first select-res) => '(6 1 2 5 3 4))
    (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(4 3 5 2 1 6)))

  ;; select and rselect starting from an upper bound
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  6 {:client "1"})
           (insert tx :order  5 {:client "2"})
           (insert tx :order 1 {:client "2"})
           (insert tx :order 2 {:client "2"})
           (insert tx :order 3 {:client "3"})
           (insert tx :order 4 {:client "3"}))
        
        rselect-res (rselect tx :order [:client] <= ["2"])
        ]
    
    (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(5 2 1 6)))


  ;; test for string primary keys
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  "6" {:client 1})
           (insert tx :order  "5" {:client 2})
           (insert tx :order "1" {:client 2})
           (insert tx :order "2" {:client 2})
           (insert tx :order "3" {:client 3})
           (insert tx :order "4" {:client 3}))
        select-res  (select tx :order [:client] >= [1] <= [3])
        rselect-res (rselect tx :order [:client] >= [2] <= [3])
        
        ]
    (fact "select should be ordered according to order of insertion" (map first select-res) => '("6" "5" "1" "2" "3" "4"))
    (fact "rselect should be ordered according to order of insertion" (map first rselect-res) => '("4" "3" "2" "1" "5"))))

