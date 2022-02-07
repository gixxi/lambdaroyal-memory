(ns lambdaroyal.memory.core.test-tx-indexing
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))

(facts "check select and rselect ordering after insert into collection with indexes"
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  6 {:client 1})
           (insert tx :order  3 {:client 2})
           (insert tx :order 2 {:client 2})
           (insert tx :order 4 {:client 2})
           (insert tx :order 7 {:client 3})
           (insert tx :order 1 {:client 3}))
        _ (doseq  [x (-> (applicable-indexes (-> @ctx :order) [:client]) first .get-data)]
            (println :x x)
            )
        select-res  (select tx :order [:client] >= [0] <= [3])
        _ (println :select-res)
        _ (doseq [x select-res]
            (println x))
        rselect-res (rselect tx :order [:client] >= [0] <= [3])
        _ (println :rselect-res)
        _ (doseq [x rselect-res]
            (println x))
        rselect-res' (rselect tx :order [:client] <= [3] >= [0])]
    (fact "select should be ordered according to index by ascending primary key" (map first select-res) => '(6 2 3 4 1 7))
    (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(7 1 4 3 2 6))
    (fact "rselect' should be ordered by descending primary key" (map first rselect-res) => '(7 1 4 3 2 6)))

  ;; Testing for string primary keys
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order  6 {:client 1})
           (insert tx :order  5 {:client 2})
           (insert tx :order 1 {:client 2})
           (insert tx :order 2 {:client 2})
           (insert tx :order 3 {:client 3})
           (insert tx :order 4 {:client 3}))
        _ (doseq  [x (-> (applicable-indexes (-> @ctx :order) [:client]) first .get-data)]
            (println :x x)
            )
        select-res  (select tx :order [:client] >= [0] <= [3])
        _ (println :select-res)
        _ (doseq [x select-res]
            (println x))
        rselect-res (rselect tx :order [:client] >= [0] <= [3])
        _ (println :rselect-res)
        _ (doseq [x rselect-res]
            (println x))
        rselect-res' (rselect tx :order [:client] <= [3] >= [0])]
    (comment (fact "select should be ordered according to index by ascending primary key" (map first select-res) => '( 6 2 3 4 1 7))
             (fact "rselect should be ordered by descending primary key" (map first rselect-res) => '(6 4 3 2 7 1))
             (fact "rselect' should be ordered by descending primary key" (map first rselect-res) => '(6 4 3 2 7 1)))))

;; test for string primary keys
