(ns lambdaroyal.memory.abstraction.test-search
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

(facts "checking hierarchie builder"
  (let [data [[1 {:size :big :color :red}] [2 {:size :big :color :green}]]]
    (fact (hierarchie data identity #(-> % last :size) #(-> % last :col)) => '([[:big 2] ([[nil 2] [[1 {:color :red, :size :big}] [2 {:color :green, :size :big}]]])]))
    (fact (hierarchie data identity #(-> % last :size) #(-> % last :col)) => '([[:big 2] ([[nil 2] [[1 {:color :red, :size :big}] [2 {:color :green, :size :big}]]])]))
    (fact (hierarchie data identity #(-> % last :size) (fn [d] (get (last d) :color))) => '([[:big 2] ([[:red 1] [[1 {:color :red, :size :big}]]] [[:green 1] [[2 {:color :green, :size :big}]]])]))
    (fact (hierarchie data #(count %) #(-> % last :size) (fn [d] (get (last d) :color))) => '([[:big 2] ([[:red 1] 1] [[:green 1] 1])]))))


(defn shortpath [[x y]]
  [#(-> % last x) #(-> % last y)])

(facts "checking hierarchie builder with category characteristics"
  (let [data [[1 {:size :big :shape :cube :color :red :alpha :light}] [2 {:size :big :shape :cube :color :green :alpha :dark}]]]
    (fact "searching by site and shape - verbose option"
      (hierarchie-ext data identity [#(-> % last :size) #(-> % last :shape)] [#(-> % last :color) #(-> % last :alpha)]) => '([[:big 2 :cube] ([[:red 1 :light] [[1 {:color :red, :size :big, :shape :cube, :alpha :light}]]] [[:green 1 :dark] [[2 {:color :green, :size :big, :shape :cube, :alpha :dark}]]])]))
    (fact "searching by site and shape - less verbose option"
      (apply hierarchie-ext data identity (map #(shortpath %) [[:size :shape] [:color :alpha]])) 
      => '([[:big 2 :cube] ([[:red 1 :light] [[1 {:color :red, :size :big, :shape :cube, :alpha :light}]]] [[:green 1 :dark] [[2 {:color :green, :size :big, :shape :cube, :alpha :dark}]]])]))))
















