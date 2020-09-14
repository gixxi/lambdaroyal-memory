(ns lambdaroyal.memory.core.test-reverse_seq 
  "tests index lockups and range scans in reverse order"
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))


(let [ctx (create-context meta-model)
      tx (create-tx ctx)
      counter (ref 0)]
  (fact "[REVERSE SEQ] check select-first, select, delete-by-select and inserting to non-unique collections"
         (dosync
          (doall (repeatedly 1000 #(insert tx :order (alter counter inc) {:orell :meisi :anne :iben})))
          (doall (repeatedly 2 #(insert tx :interaction (alter counter inc) {})))
          (fact "[REVERSE SEQ]select all elements must reveal all elements"
                (count (rselect tx :order >= -10)) => 1000)
          (let [xs (rselect tx :order)]
            (fact "[REVERSE SEQ] select all elements without constraining must reveal all elements"
                  (count xs) => 1000)
            (fact "[REVERSE SEQ] select all elements must yield reverse order"
                  (map first xs) => (reverse (range 1 1001))))
          (let [xs (rselect tx :order > 1)]
            (fact "[REVERSE SEQ]select all but the first elements ..."
                  (count xs) => 999)
            (fact "[REVERSE SEQ]select all but the first elements yields reverse order..."
                  (map first xs) => (reverse (range 2 1001))))
          (let [xs (rselect tx :order >= 500 < 700)]
            (fact "[REVERSE SEQ]select subset ..."
                  (count xs) => 200)            
            (fact "[REVERSE SEQ]select subset yields reverse order ..."
                  (map first xs) => (reverse (range 500 700))))
          (let [xs (rselect tx :order >= 500 = 501)]
            (fact "[REVERSE SEQ]select subset ..."
                  (count xs) => 2)            
            (fact "[REVERSE SEQ]select subset yields reverse order ..."
                  (map first xs) => [501 500])))))

(fact "[REVERSE SEQ] check insert into collection with indexes"
       (let [ctx (create-context meta-model-with-indexes)
             tx (create-tx ctx)
             _ (dosync
                (doseq [i (range 1000)]
                  (insert tx :order i {:type :test :keyword i :client (mod i 2) :number i})))
             idx-client (-> ctx deref :order :constraints deref :client)
             idx-client-no (-> ctx deref :order :constraints deref :client-no)
             timed-find (timed
                         (.rfind idx-client >= [0] < [1])) 
             timed-auto-find (timed
                              (rselect tx :order [:client] >= [0] < [1]))
             timed-select (timed
                           (doall
                            (filter #(= (-> % last :client) 0) (rselect tx :order >= 0))))
             _ (println "count for timed-find" (-> timed-find last count))
             _ (println "count for timed-auto-find" (-> timed-auto-find last count))
             _ (println "count for select" (-> timed-select last count))
             _ (println "time for find 500 of 1000 using index" (first timed-find))
             _ (println "time for filter 500 of 1000" (first timed-select))
             _ (println "time for auto-select 500 of 1000" (first timed-auto-find))]

         (fact "[REVERSE SEQ]index :client reveals 500 entries"
               (count (.rfind idx-client >= [0] < [1])) => 500)

         (fact "[REVERSE SEQ]index :client reveals 500 entries"
               (distinct (map (comp :client last user-scope-tuple) (.rfind idx-client >= [0] < [1]))) => '(0))
         (fact "[REVERSE SEQ]index :client reveals 500 entries"
               (distinct (map (comp :client last user-scope-tuple) (.rfind idx-client >= [1] = [1]))) => '(1))
         (fact "[REVERSE SEQ]index :client reveals 500 entries in reverse orer"
               (map (comp first user-scope-tuple) (.rfind idx-client >= [1] = [1])) => (reverse (range 1 998 2)))
         (fact "[REVERSE SEQ]finding using index must outperform non-index filter by factor 5"
               (< (* 5 (first timed-find))
                  (first timed-select))
               => truthy)
         (fact "[REVERSE SEQ]finding using auto-selected index must outperform non-index filter by factor 5"
               (< (* 5 (first timed-auto-find))
                  (first timed-select)))
         (fact "[REVERSE SEQ]find and select must reveal the same items"
               (-> timed-find last count) =>
               (-> timed-select last count))
         (fact "[REVERSE SEQ]find and auto-find must reveal the same items"
               (-> timed-find last count) =>
               (-> timed-auto-find last count))))

(fact "[REVERSE SEQ] check start and stop condition for indexes"
       (let [ctx (create-context meta-model-with-indexes)
             tx (create-tx ctx)
             _ (dosync
                (insert tx :order 0 {:type :test  :number 5})
                (insert tx :order 1 {:type :test :client "client1" :bool false :number 2})
                (insert tx :order 2 {:type :test :client "client1" :bool true :number 5})
                (insert tx :order 3 {:type :test :client "client0" :bool true :number 1})
                (insert tx :order 4 {:type :test :client "client4"})
                (insert tx :order 5 {:type :test :bool false}))] 
         ;; strings
         (do
           (fact "[REVERSE SEQ]start cond >= nil must reveal all" 
                 (map #(first %) (rselect tx :order [:client] >= [nil])) => '(4 2 1 3 5 0))
           (fact "[REVERSE SEQ]start cond > nil" 
                 (map #(first %) (rselect tx :order [:client] > [nil])) => '(4 2 1 3))
           (fact "[REVERSE SEQ]start cond >= non-existing" (map #(first %) (rselect tx :order [:client] >= ["client"])) => '(4 2 1 3))
           (fact "[REVERSE SEQ]start cond > non-existing" (map #(first %) (rselect tx :order [:client] > ["client"])) => '(4 2 1 3))
           (fact "[REVERSE SEQ]start cond >= largest" 
                 (map #(first %) (rselect tx :order [:client] >= ["client4"])) => '(4))
           (fact "[REVERSE SEQ]start cond >= existing" 
                 (map #(first %) (rselect tx :order [:client] > ["client4"])) => '())
           (fact "[REVERSE SEQ]start cond >= exceed upper bound" 
                 (map #(first %) (rselect tx :order [:client] > ["client5"])) => '())
           (fact "[REVERSE SEQ]start cond >= non-existing = existing" (map #(first %) (rselect tx :order [:client] >= ["client"] < ["client1"])) => '(3))
           (fact "[REVERSE SEQ]start cond >= non-existing = existing" (map #(first %) (rselect tx :order [:client] >= ["client"] <= ["client1"])) => '(2 1 3)))))
