(ns lambdaroyal.memory.abstraction.test-search-abstraction
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.helper :refer :all]
           [lambdaroyal.memory.abstraction.search :refer :all]
           [clojure.test :refer :all])
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
       (map (fn [x] [(first x) (assoc (last x) :coll coll-name)]) (select tx coll-name >= query))))))

(defn search-top2-coll [ctx coll-name]
  (abstract-search 
   (fn [query]
     (let [tx (create-tx ctx)]
       (take 2 (map (fn [x] [(first x) (assoc (last x) :coll coll-name)]) (select tx coll-name >= query)))))))

(facts "search abstractions - using std aggregator functions"
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
    (fact "concat aggregation - double output by cloning all the queries"
      (let [result (ref [])
            result-promise (promise)
            consum (promise)
            start (System/currentTimeMillis)]
        (do (combined-search [(search-coll ctx :a)
                              (search-coll ctx :a)
                              (search-coll ctx :b)
                              (search-coll ctx :b)
                              (search-coll ctx :c)
                              (search-coll ctx :c)]
                             (partial concat-aggregator result) 0
                             :timeout 1000
                             :finish-callback (fn [] (do
                                                       (println "concat aggregation took (ms) " (- (System/currentTimeMillis) start))
                                                       (deliver result-promise @result))))
            (count @result-promise))) => 60000)
    (fact "set aggregation"
      (let [result (ref #{})
            result-promise (promise)
            consum (promise)
            start (System/currentTimeMillis)]
        (do (combined-search [(search-coll ctx :a)
                              (search-coll ctx :a)
                              (search-coll ctx :b)
                              (search-coll ctx :b)
                              (search-coll ctx :c)
                              (search-coll ctx :c)]
                             (partial set-aggregator result) 0
                             :timeout 1000
                             :finish-callback (fn [] (do
                                                       (println "set concat aggregation took (ms) " (- (System/currentTimeMillis) start))
                                                       (deliver result-promise @result))))
            (count @result-promise))) => 30000)
    (let [result (gen-sorted-set)
          result-promise (promise)
          consum (promise)
          start (System/currentTimeMillis)
          _ (combined-search [(search-top2-coll ctx :a)
                              (search-top2-coll ctx :b)
                              (search-top2-coll ctx :b)]
                             (comp (partial set-aggregator result) #(do (println :top2 %) %)) 0
                             :timeout 1000
                             :finish-callback (fn [] (do
                                                       (println "sorted set aggregation on 6 documents took (ms) " (- (System/currentTimeMillis) start))
                                                       (deliver result-promise @result))))]
      (fact (count @result-promise) => 4)
      (fact "check sorting (id)" (-> @result-promise first first) => 0)
      (fact "check sorting (coll)" (-> @result-promise first last :coll) => :a))
    (fact "sorted set aggregation"
      (let [result (gen-sorted-set)
            result-promise (promise)
            consum (promise)
            start (System/currentTimeMillis)]
        (do (combined-search [(search-coll ctx :a)
                              (search-coll ctx :a)
                              (search-coll ctx :b)
                              (search-coll ctx :b)
                              (search-coll ctx :c)
                              (search-coll ctx :c)]
                             (partial set-aggregator result) 0
                             :timeout 3000
                             :finish-callback (fn [] (do
                                                       (println "sorted set concat aggregation took (ms) " (- (System/currentTimeMillis) start))
                                                       (deliver result-promise @result))))
            (count @result-promise))) => 30000)))















