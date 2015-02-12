(ns lambdaroyal.memory.core.test-tx
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.test-context :refer [meta-model]]
           [lambdaroyal.memory.helper :refer :all])
  (import [lambdaroyal.memory.core ConstraintException])
  (:gen-class))


(facts "check insert into collection with unique key constraint"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order :a {:type :test}))
        _ (dosync
           (insert tx :order :b {:type :pro}))
        _ (-> ctx deref :order :data deref println)
        _ (println (find-first (-> ctx deref :order) :b))]
    (fact "can insert value at all"
      (contains-key? (-> ctx deref :order) :a) => truthy)
    (fact "can insert a second value at all"
      (contains-key? (-> ctx deref :order) :b) => truthy)
    (fact "can not insert on existing key within a new transaction"
      (dosync
       (insert tx :order :a {})) => (throws ConstraintException))
    (fact "two inserts in one transaction are treated atomically"
      (dosync
       (insert tx :order :c {})
       (insert tx :order :c {})) => (throws ConstraintException #".+?unique key constraint violated.*"))))

(defn- insert-future [ctx prefix]
  "spawns a thread that starts 100 consecutive transactions, each transaction consists of 10 inserts"
  (future
    (let [tx (create-tx ctx)]
      (doseq [p (partition 10 (range 1000))]
        (dosync
         (doseq [i p]
           (insert tx :order (format "%d:%d" prefix i) {})))))
    prefix))

(facts "check timing constraints inserting 10000 items with 10 futures, each future executing 1000 transaction a 10 inserts"
  (let [ctx (create-context meta-model)
        threads 10
        bulk (timed
              (reduce (fn [acc i] (+ acc @i)) 0 (pmap #(insert-future ctx %) (range threads))))
        _ (println :bulk bulk)]
    (fact "all elements inserted" (-> ctx deref :order :data deref count) => (* 1000 threads))
    (fact "all threads finished" (float (last bulk)) => (Math/floor (/ (* threads (dec threads)) 2)))
    (fact "max time for 10000 parallel inserts is 1000ms" (first bulk) => (roughly 0 1000))))




















