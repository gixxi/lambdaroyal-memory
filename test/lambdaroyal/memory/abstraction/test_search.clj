(ns lambdaroyal.memory.abstraction.test-search
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.helper :refer :all]
           [lambdaroyal.memory.abstraction.search :refer :all]
           [clojure.test :refer :all])
  (import  [lambdaroyal.memory.core ConstraintException]))

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

(facts "checking hierarchie builder for distinction in first group"
  (fact (hierarchie [[1 {:size :big :color :red}] [2 {:size :small :color :green}]] identity #(-> % last :size)) =>
        '([[:big 1] [[1 {:color :red, :size :big}]]] [[:small 1] [[2 {:color :green, :size :small}]]])))

(facts "checking hierarchie builder for distinction in first group and aggregator handler"
  (fact (hierarchie [[1 {:size :big :color :red}] [2 {:size :small :color :green}] [3 {:size :small :color :green}]] #(count %) #(-> % last :size)) => '([[:big 1] 1] [[:small 2] 2])))

(facts "checking hierarchie builder"
  (let [data [[1 {:size :big :color :red}] [2 {:size :big :color :green}]]]
    (fact (hierarchie data identity #(-> % last :size) #(-> % last :col)) =>
          [[:big 2] [[1 {:color :red, :size :big}] [2 {:color :green, :size :big}]]])
    (fact (hierarchie data identity #(-> % last :size) #(-> % last :color)) => 
          [[:big 2] '([[:red 1] [[1 {:color :red, :size :big}]]] [[:green 1] [[2 {:color :green, :size :big}]]])])
    (fact (hierarchie data identity #(-> % last :size) (fn [d] (get (last d) :color))) => [[:big 2] '([[:red 1] [[1 {:color :red, :size :big}]]] [[:green 1] [[2 {:color :green, :size :big}]]])])
    (fact (hierarchie data #(count %) #(-> % last :size) (fn [d] (get (last d) :color))) => [[:big 2] '([[:red 1] 1] [[:green 1] 1])])))


(facts "checking hierarchie builder with backtracking"
  (let [data [{:size :big :color :red :length 1} {:size :big :color :green :length 2}{:size :big :color :green :length 3} {:size :huge :length 100}]]
    (fact (hierarchie-backtracking data identity
                                   (fn [leaf k xs]
                                     (if leaf 
                                       (conj k (apply + (map :length xs)))
                                       (conj k (apply + (map #(-> % first last) xs))))
                                     ) 
                                   :size :color) =>
                                   '([[:big 3 6] ([[:red 1 1] [{:color :red, :length 1, :size :big}]] [[:green 2 5] [{:color :green, :length 2, :size :big} {:color :green, :length 3, :size :big}]])] [[:huge 1 100] [{:length 100, :size :huge}]]))))

(facts "checking hierarchie builder with backtracking with partial hierarchie"
  (let [data [{:size :big :color :red :length 1} {:size :big :color :green :length 2}{:size :big :color :green :length 3} {:color :blue :length 100}]]
    (fact (time (hierarchie-backtracking data identity
                                         (fn [leaf k xs]
                                           (if leaf 
                                             (conj k (apply + (map :length xs)))
                                             (conj k (apply + (map #(-> % first last) xs))))
                                           ) 
                                         :size :color)) =>
                                   '([[:big 3 6] ([[:red 1 1] [{:color :red, :length 1, :size :big}]] [[:green 2 5] [{:color :green, :length 2, :size :big} {:color :green, :length 3, :size :big}]])] [[:blue 1 100] [{:color :blue, :length 100}]]))))

(defn shortpath [[x y]]
  [#(-> % last x) #(-> % last y)])
(defn shortpath2 [x]
  #(-> % last x))

(facts "checking hierarchie builder with category characteristics"
  (let [data [[1 {:size :big :color :red}] [2 {:size :big :color :green}]
              [3 {:size :small}]]
        res (hierarchie data identity #(-> % last :size) #(-> % last :color))
        _ (println :res res)]
    (fact "build hierarchies with partial information" res => '([[:big 2] ([[:red 1] [[1 {:color :red, :size :big}]]] [[:green 1] [[2 {:color :green, :size :big}]]])] [[:small 1] [[3 {:size :small}]]])))
  (let [data '(
               [1 {:_id 1  :unique-key 1 :bereich "WE"  :count-stock 5}] 
               [2 {:_id 2 :gang "gang" :feld "feld" 1  :ebene "ebene" 2 :unique-key 2 :bereich "kühl"  :count-stock 1 :seite "links"}] 
               [3 {:_id 3 :gang "gang" 1 :feld "feld 1"  :ebene "ebene" 3 :unique-key 3 :bereich "kühl"  :count-stock 0 :seite links}] [4 {:_id 4 :gang "gang 1" :feld "feld 2" :ebene "ebene 1" :unique-key 4, :bereich "kühl"  :count-stock 1 :seite "links"}])
        res (apply hierarchie data
                   #(apply + (map (fn [x] (-> x last :count-stock)) %))
                   (map #(shortpath2 %) [:bereich :gang]))
        _ (println :res res)]
    (fact "build hierarchies with partial information and leaf handler" res => '([["WE" 1] 5] [["kühl" 3] ([["gang" 2] 1] [["gang 1" 1] 1])])))
  (let [data [[1 {:size :big :shape :cube :color :red :alpha :light}] [2 {:size :big :shape :cube :color :green :alpha :dark}]]]
    (fact "searching by site and shape - verbose option"
      (hierarchie-ext data identity [#(-> % last :size) #(-> % last :shape)] [#(-> % last :color) #(-> % last :alpha)]) => '([[:big 2 :cube] ([[:red 1 :light] [[1 {:color :red, :size :big, :shape :cube, :alpha :light}]]] [[:green 1 :dark] [[2 {:color :green, :size :big, :shape :cube, :alpha :dark}]]])]))
    (fact "searching by site and shape - less verbose option"
      (apply hierarchie-ext data identity (map #(shortpath %) [[:size :shape] [:color :alpha]])) 
      => '([[:big 2 :cube] ([[:red 1 :light] [[1 {:color :red, :size :big, :shape :cube, :alpha :light}]]] [[:green 1 :dark] [[2 {:color :green, :size :big, :shape :cube, :alpha :dark}]]])]))))
