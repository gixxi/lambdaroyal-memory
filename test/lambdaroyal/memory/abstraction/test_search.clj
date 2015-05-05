(ns lambdaroyal.memory.abstraction.test-search
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.helper :refer :all]
           [lambdaroyal.memory.abstraction.search :refer :all])
  (import [lambdaroyal.memory.core ConstraintException]))


(def meta-model
  {:a
   {:unique true :indexes []}
   :b
   {:unique true :indexes []}
   :c
   {:unique true :indexes []}})

(defn search-coll [ctx coll-name]
  (abstract-search 
   (fn [query]
     (let [tx (create-tx ctx)]
       (select tx coll-name >= query)))))

(facts "doing parallel combined search using core/async"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        warmup (timed (doseq [r 
                              (pmap 
                               #(future 
                                  (dosync 
                                   (doseq [r (range 10000)]
                                     (insert tx % r {:foo :bar :i r :coll %}))))
                               '(:a :b :c))]
                        @r))
        _ (println "warmup took (ms) " (first warmup))]
    (fact "doing searching for all without time constraints must return 30000 elems"
      (let [result (atom [])
            result-promise (promise)
            aggregator (fn [n] (swap! result concat n))]
        (combined-search [(search-coll ctx :a)
                          (search-coll ctx :b)
                          (search-coll ctx :c)]
                         aggregator 0
                         :finish-callback (fn [] (deliver result-promise @result)))
        (count @result-promise)) => 30000)

    (fact "doing searching for all without time constraints but with minority (2) report must return 20000 elems"
      (let [result (atom [])
            result-promise (promise)
            aggregator (fn [n] (swap! result concat n))]
        (combined-search [(search-coll ctx :a)
                          (search-coll ctx :b)
                          (search-coll ctx :c)]
                         aggregator 0
                         :minority-report 2
                         :finish-callback (fn [] (deliver result-promise @result)))
        (count @result-promise)) => 20000)
    (fact "doing searching for all without time constraints but with minority (1) report must return 10000 elems"
      (let [result (atom [])
            result-promise (promise)
            aggregator (fn [n] (swap! result concat n))]
        (combined-search [(search-coll ctx :a)
                          (search-coll ctx :b)
                          (search-coll ctx :c)]
                         aggregator 0
                         :minority-report 1
                         :finish-callback (fn [] (deliver result-promise @result)))
        (count @result-promise)) => 10000)))












