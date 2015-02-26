(ns lambdaroyal.memory.core.test-tx
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes]]
           [lambdaroyal.memory.helper :refer :all])
  (import [lambdaroyal.memory.core ConstraintException]))

(facts "check core multimethods find-first/contains-key?"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order :a {:type :test})
           (insert tx :order :b {:type :pro}))
        coll (-> ctx deref :order)
        tuple-a-by-user-key (find-first coll :a)
        tuple-a-by-key (find-first coll (first tuple-a-by-user-key))]
    (fact "find-first by user key reveals an element"
      tuple-a-by-user-key => truthy)
    (fact "find-first by user key reveals the proper element"
      (-> tuple-a-by-user-key user-scope-tuple last) => {:type :test})
    (fact "find-first by key reveals an element"
      tuple-a-by-key => truthy)
    (fact "find-first by key reveals the proper element"
      tuple-a-by-key => tuple-a-by-user-key)
    (fact "contains-key? by user key reveals element"
      (contains-key? coll :a) => truthy)
    (fact "contains-key? by key reveals element"
      (contains-key? coll (first tuple-a-by-key)) => truthy)))

(facts "check multimethod delete"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order :a {:type :test})
           (insert tx :order :b {:type :pro}))
        coll (-> ctx deref :order)
        tuple-a-by-user-key (find-first coll :a)
        tuple-a-by-key (find-first coll (first tuple-a-by-user-key))
        _ (println :tuple-by-unique-key tuple-a-by-key)
        _ (println :tuple-by-user-key tuple-a-by-user-key)]
    (fact "delete by user key the first time succeeds"
      (dosync (delete tx :order :b)) => 1)
    (fact "delete by user key the second time fails"
      (dosync (delete tx :order :b)) => 0)
    (fact "delete by unique key the first time succeeds"
      (dosync (delete tx :order (first tuple-a-by-key))) => 1)
    (fact "delete by unique key the first time fails"
      (dosync (delete tx :order (first tuple-a-by-key))) => 0)))

(facts "check insert into collection with unique key constraint"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync
           (insert tx :order :a {:type :test}))
        _ (dosync
           (insert tx :order :b {:type :pro}))]
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

(let [ctx (create-context meta-model)
      tx (create-tx ctx)]
  (facts "check delete and coll-empty?"
    (dosync
     (insert tx :order :a {:type :test})
     (insert tx :order :b {:type :pro}))
    (fact "after insert the collection is not empty"
      (coll-empty? tx :order) => falsey))
  
  (fact "removing successfully 1 item must return 1" (dosync (delete tx :order :a)) => 1)
  (fact "removing successfully 1 item must return 1" (dosync (delete tx :order :b)) => 1)
  (fact "removing twice the same item must not work" (dosync (delete tx :order :b)) => 0)
  (fact "removing non existing item must not work" (dosync (delete tx :order :foo)) => 0)
  (fact "after removing all the list must be empty"
    (coll-empty? tx :order) => truthy))

(let [ctx (create-context meta-model)
      tx (create-tx ctx)
      counter (ref 0)]
  (facts "check select-first, select, delete-by-select and inserting to non-unique collections"
    (dosync
     (doall (repeatedly 1000 #(insert tx :order (alter counter inc) {:orell :meisi :anne :iben})))
     (doall (repeatedly 2 #(insert tx :interaction 1 {})))
     (fact "select first on existing must work"
       (-> (select-first tx :order 500) last) => {:orell :meisi :anne :iben})
     (fact "select all elements must reveal all elements"
       (count (select tx :order >= -10)) => 1000)
     (fact "select all but the first elements ..."
       (count (select tx :order > 1)) => 999)
     (fact "select subset ..."
       (count (select tx :order >= 500 < 700)) => 200)
     (doseq [i (select tx :order >= 500 < 700)]
       (delete tx :order (-> i first first)))
     (fact "delete-by-select must reveal intersection"
       (count (select tx :order > 0)) => 800))))

(facts "check insert into collection with indexes"
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (doseq [i (range 1000)]
             (insert tx :order i {:type :test :keyword i :client (mod i 2) :number i})))
        idx-client (-> ctx deref :order :constraints deref :client)
        idx-client-no (-> ctx deref :order :constraints deref :client-no)
        timed-find (timed
                    (.find idx-client >= [0] < [1])) 
        timed-auto-find (timed
                         (select tx :order [:client] >= [0] < [1]))
        timed-select (timed
                      (doall
                       (filter #(= (-> % last :client) 0) (select tx :order >= 0))))
        _ (println "count for timed-find" (-> timed-find last count))
        _ (println "count for timed-auto-find" (-> timed-auto-find last count))
        _ (println "count for select" (-> timed-select last count))
        _ (println "time for find 500 of 1000 using index" (first timed-find))
        _ (println "time for filter 500 of 1000" (first timed-select))
        _ (println "time for auto-select 500 of 1000" (first timed-auto-find))]
    (fact "index :client must be present" idx-client => truthy)
    (fact "index :client-no must be present" idx-client-no => truthy)
    (fact "index :client is applicable on search attributes '(:client)"
      (.applicable? idx-client '(:client)) => truthy)
    (fact "index :client is applicable on search attributes [:client]"
      (.applicable? idx-client [:client]) => truthy)
    (fact "index :client is applicable on search attributes [:foo]"
      (.applicable? idx-client [:foo]) => falsey)
    (fact "index :client-no is applicable on search attributes [:client]"
      (.applicable? idx-client-no [:client]) => truthy)
    (fact "index :client-no is applicable on search attributes [:client :no]"
      (.applicable? idx-client-no [:client :no]) => truthy)
    (fact "index :client-no is applicable on search attributes [:client]"
      (.applicable? idx-client-no [:no :client]) => falsey)
    (fact "index :client reveals 500 entries"
      (count (.find idx-client >= [0] < [1])) => 500)
    (fact "finding using index must outperform non-index filter by factor 10"
      (< (* 10 (first timed-find))
         (first timed-select))
      => truthy)
    (fact "finding using auto-selected index must outperform non-index filter by factor 10"
      (< (* 10 (first timed-auto-find))
         (first timed-select)))
    (fact "find and select must reveal the same items"
      (-> timed-find last count) =>
      (-> timed-select last count))
    (fact "find and auto-find must reveal the same items"
      (-> timed-find last count) =>
      (-> timed-auto-find last count))))


(facts "altering indexed elements"
  (let [ctx (create-context meta-model-with-indexes)
        tx (create-tx ctx)
        _ (dosync
           (doseq [i (range 10)]
             (insert tx :order i {:type :test :keyword i :client (mod i 2) :number i})))
        r (select tx :order [:client] >= [0] < [3])
        _ (println :r r)]
    (fact "can find item at all" r => truthy)))














