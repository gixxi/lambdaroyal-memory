(ns lambdaroyal.memory.core.test-tx
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.test-context :refer [meta-model meta-model-with-indexes meta-model-with-ric meta-model-with-ric']]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]))

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
               (dissoc (-> tuple-a-by-user-key user-scope-tuple last) :vlicGtid) => {:type :test})
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
         (fact "delete by unique key the second time fails"
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
          (doall (repeatedly 2 #(insert tx :interaction (alter counter inc) {})))
          (fact "select first on existing must work"
                (dissoc (-> (select-first tx :order 500) last) :vlicGtid) => {:orell :meisi :anne :iben})
          (fact "select all elements must reveal all elements"
                (count (select tx :order >= -10)) => 1000)
          (fact "select all elements without constraining must reveal all elements"
                (count (select tx :order)) => 1000)
          (fact "select all but the first elements ..."
                (count (select tx :order > 1)) => 999)
          (fact "select subset ..."
                (count (select tx :order >= 500 < 700)) => 200)
          (doseq [i (select tx :order >= 500 < 700)]
            (delete tx :order (-> i first)))
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
    (fact "finding using index must outperform non-index filter by factor 5"
      (< (* 5 (first timed-find))
         (first timed-select))
      => truthy)
    (fact "finding using auto-selected index must outperform non-index filter by factor 5"
      (< (* 5 (first timed-auto-find))
         (first timed-select)))
    (fact "find and select must reveal the same items"
      (-> timed-find last count) =>
      (-> timed-select last count))
    (fact "find and auto-find must reveal the same items"
      (-> timed-find last count) =>
      (-> timed-auto-find last count))))

(facts "inserting and removing elements from an index-backed collection must alter back the index as well"
       (let [ctx (create-context meta-model-with-indexes)
             tx (create-tx ctx)
             _ (dosync
                (insert tx :order 1 {:type :test :keyword "gaga" :client 1 :no 2})
                (insert tx :order 2 {:type :test :keyword "gaga" :client 2 :no 2})
                )
             idx-client (-> ctx deref :order :constraints deref :client)
             idx-client-no (-> ctx deref :order :constraints deref :client-no)] 
         (fact "can find item at all using index 1" (first (select tx :order [:client] >= [1] < [2])) => truthy)
         (fact "can find item at all using index 2" (first (select tx :order [:client :number] >= [1 2] < [1 3])) => truthy)
         (dosync
          (delete tx :order 1))
         (fact "must not find item at all using index 1 after removal" (first (select tx :order [:client] >= [1] < [2])) => falsey)
         (fact "must not find item at all using index 2 after removal" (first (select tx :order [:client :number] >= [1 2] < [1 3])) => falsey)))

(facts "check start and stop condition for indexes"
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
           (fact "start cond >= nil must reveal all" (into #{} (map #(first %) (select tx :order [:client] >= [nil]))) => #{0 1 2 3 4 5})
           (fact "start cond > nil" (into #{} (map #(first %) (select tx :order [:client] > [nil]))) => #{1 2 3 4})
           (fact "start cond >= non-existing" (into #{} (map #(first %) (select tx :order [:client] >= ["client"]))) => #{1 2 3 4})
           (fact "start cond > non-existing" (into #{} (map #(first %) (select tx :order [:client] > ["client"]))) => #{1 2 3 4})
           (fact "start cond >= largest" (into #{} (map #(first %) (select tx :order [:client] >= ["client4"]))) => #{4})
           (fact "start cond >= existing" (into #{} (map #(first %) (select tx :order [:client] > ["client4"]))) => #{})
           (fact "start cond >= exceed upper bound" (into #{} (map #(first %) (select tx :order [:client] > ["client5"]))) => #{})
           (fact "start cond <= exceed upper bound" (into #{} (map #(first %) (select tx :order [:client] <= ["client5"]))) => #{0 1 2 3 4 5})
           (fact "start cond < exceed upper bound" (into #{} (map #(first %) (select tx :order [:client] < ["client5"]))) => #{0 1 2 3 4 5})
           (fact "start cond <= below lower bound" (into #{} (map #(first %) (select tx :order [:client] <= ["client"]))) => #{0 5})
           (fact "start cond < below upper bound" (into #{} (map #(first %) (select tx :order [:client] < ["client"]))) => #{0 5}))
         ;; numbers
         (do
           (fact "start cond >= nil must reveal all" (into #{} (map #(first %) (select tx :order [:number] >= [nil]))) => #{0 1 2 3 4 5})
           (fact "start cond > nil" (into #{} (map #(first %) (select tx :order [:number] > [nil]))) => #{0 1 2 3})
           (fact "start cond >= non-existing" (into #{} (map #(first %) (select tx :order [:number] >= [-1]))) => #{0 1 2 3})
           (fact "start cond > non-existing" (into #{} (map #(first %) (select tx :order [:number] > [-1]))) => #{0 1 2 3})
           (fact "start cond >= largest" (into #{} (map #(first %) (select tx :order [:number] >= [5]))) => #{0 2})
           (fact "start cond >= existing" (into #{} (map #(first %) (select tx :order [:number] > [1]))) => #{0 1 2})
           (fact "start cond >= exceed upper bound" (into #{} (map #(first %) (select tx :order [:number] > [10]))) => #{})
           (fact "start cond <= exceed upper bound" (into #{} (map #(first %) (select tx :order [:number] <= [10]))) => #{0 1 2 3 4 5})
           (fact "start cond < exceed upper bound" (into #{} (map #(first %) (select tx :order [:number] < [10]))) => #{0 1 2 3 4 5})
           (fact "start cond <= below lower bound" (into #{} (map #(first %) (select tx :order [:number] <= [1]))) => #{3 4 5})
           (fact "start cond < exceed upper bound" (into #{} (map #(first %) (select tx :order [:number] < [1]))) => #{4 5}))))

(facts "altering indexed elements"
       (let [ctx (create-context meta-model-with-indexes)
             tx (create-tx ctx)
             _ (dosync
                (insert tx :order 1 {:type :test :keyword "gaga" :client 1 :no 2})
                (insert tx :order 2 {:type :test :keyword "gaga" :client 2 :no 2})
                )
             idx-client (-> ctx deref :order :constraints deref :client)
             idx-client-no (-> ctx deref :order :constraints deref :client-no)
             order-1 (select-first tx :order 1)]
         (dosync
          (alter-document tx :order order-1 assoc :a :b))
         (fact "can find item at all using index 1" (first (select tx :order [:client] >= [1] < [2])) => truthy)
         (fact "can find item at all using index 2" (first (select tx :order [:client :number] >= [1 2] < [1 3])) => truthy)
         (dosync (alter-document tx :order order-1 assoc :client 3))
         (fact "must not find item at all using index 1 after altering the document" (first (select tx :order [:client] >= [1] < [2])) => falsey)
         (fact "must not find item at all using index 2 after altering the document" (first (select tx :order [:client :number] >= [1 2] < [1 3])) => falsey)
         (fact "can find item using index 1 using changed predicates" (first (select tx :order [:client] >= [3] < [4])) => truthy)
         (fact "can find item using index 2 using changed predicates" (first (select tx :order [:client :number] >= [3 2] < [4 3])) => truthy)
         (dosync (alter-document tx :order order-1 assoc :no 5))
         (fact "must not find item using index 2 using changed predicates" (first (select tx :order [:client :number] >= [3 2] < [3 3])) => falsey)
         (fact "must not find item using index 2 using changed predicates" (first (select tx :order [:client :number] >= [3 3] < [3 6])) => truthy)))

(facts "facts abount context creation with referential integrity constraints (RIC)"
       (let [rics (map #(-> % last .name) (referential-integrity-constraint-factory meta-model-with-ric))
             ctx (create-context meta-model-with-ric)
             tx (create-tx ctx)]
         (fact "must not insert part order with inexisting type"
               (dosync
                (insert tx :part-order 1 {:type 1})) => (throws ConstraintException #".+?integrity constraint violated.*"))
         (dosync (insert tx :type 1 {})
                 (insert tx :type 2 {}))
         (fact "must not insert part order with inexisting order"
               (dosync
                (insert tx :part-order 1 {:type 1 :order 1})) => (throws ConstraintException #".+?integrity constraint violated.*"))
         (dosync (insert tx :order 1 {:name :foo}))
         (dosync (insert tx :order 2 {:name :foo2}))
         (fact "can insert part order after ensuring foreign key constraints"
               (dosync
                (insert tx :part-order 1 {:type 1 :order 1})))
         (fact "can insert part order #2 after ensuring foreign key constraints"
               (dosync
                (insert tx :part-order 2 {:type 1 :order 1})))
         (fact "can insert part order #2 after ensuring foreign key constraints"
               (dosync
                (insert tx :part-order 3 {:type 2 :order 2})))
         (fact "must not alter part order to refer a non existing type"
               (dosync
                (alter-document tx :part-order (select-first tx :part-order 1) assoc :type 3)) => (throws ConstraintException #".+?integrity constraint violated.*"))
         (fact "part-order must be unchanged after transaction failed"
               (-> (select-first tx :part-order 1) last :type) => 1)
         (fact "must not delete order with referencing part orders"
               (dosync
                (delete tx :order 1)) => (throws ConstraintException #".+?integrity constraint violated.*"))
         (fact "must not delete type with referencing part orders"
               (dosync
                (delete tx :type 1)) => (throws ConstraintException #".+?integrity constraint violated.*"))
         (dosync 
          (delete tx :part-order 1)
          (delete tx :part-order 2))
         (fact "delete order without referencing part orders"
               (dosync
                (delete tx :order 1)) => truthy)
         (fact "must not delete type without referencing part orders"
               (dosync
                (delete tx :type 1)) => truthy)))

(facts "facts abount using a unique index"
       (let [ctx (create-context {:order {:indexes [{:unique true :attributes [:no]}]}})
             tx (create-tx ctx)]
         (fact "must not fraud unique index"
               (dosync
                (insert tx :order 1 {:no 1})
                (insert tx :order 2 {:no 1})) => (throws ConstraintException #".+?unique index constraint violated.*"))
         (fact "must be able to use unique index"
               (dosync
                (insert tx :order 3 {:no 2})
                (insert tx :order 4 {:no 3})) => truthy)))

(fact "building the tree referencees for a user-scope-tuple" 
      (time (let [rics (map #(-> % last .name) (referential-integrity-constraint-factory meta-model-with-ric))
                  ctx (create-context meta-model-with-ric)
                  tx (create-tx ctx)
                  _ (dosync (insert tx :type 1 {})
                        (insert tx :order 1 {:name :foo})
                        (insert tx :part-order 1 {:type 1 :order 1 :gaga "baba"}))
                  part-order (select-first tx :part-order 1)
                  order (select-first tx :order 1)
                  type (select-first tx :type 1)]
              (tree-referencees tx :part-order part-order) 
              => {[:order 1] 
                  [:order [1 {:name :foo :vlicGtid (-> order last :vlicGtid)}]]
                  [:type 1] [:type [1 {:vlicGtid (-> type last :vlicGtid)}]]})))

(fact "building the tree for a user-scope-tuple (old signature)" 
      (time (let [rics (map #(-> % last .name) (referential-integrity-constraint-factory meta-model-with-ric))
                  ctx (create-context meta-model-with-ric)
                  tx (create-tx ctx)]
              (do
                (dosync (insert tx :type 1 {})
                        (insert tx :order 1 {:name :foo})
                        (insert tx :part-order 1 {:type 1 :order 1 :gaga "baba"})
                        (insert tx :line-item 1 {:no 1 :part-order 1}))
                (tree tx :line-item (select-first tx :line-item 1) {})
                => [1 {:vlicGtid (-> (select-first tx :line-item 1) last :vlicGtid)
                       :coll :line-item, :no 1, :part-order [1 {:vlicGtid (-> (select-first tx :part-order 1) last :vlicGtid)
                                                                :coll :part-order, :gaga "baba", 
                                                                :order [1 {:vlicGtid (-> (select-first tx :order 1) last :vlicGtid)
                                                                           :coll :order, :name :foo}], :type [1 {:vlicGtid (-> (select-first tx :type 1) last :vlicGtid)
                                                                                                                 :coll :type}]}]}]))))

(fact "building the tree for a user-scope-tuple and assoc referencees by attr-name" 
      (time (let [rics (map #(-> % last .name) (referential-integrity-constraint-factory meta-model-with-ric'))
                  ctx (create-context meta-model-with-ric')
                  tx (create-tx ctx)
                  type (dosync (insert tx :type 1 {}))
                  order (dosync (insert tx :order 1 {:name :foo}))
                  part-order1 (dosync (insert tx :part-order 1 {:type 1 :order 1 :gaga "baba"}))
                  part-order2 (dosync (insert tx :part-order 2 {:type 1 :order 1 :gaga "bobo"}))
                  line-item (dosync (insert tx :line-item 1 {:no 1 :part-order-original 1 :part-order-old 2}))
                  tree (tree tx :line-item (select-first tx :line-item 1) :use-attr-name true)]
              
              tree => [1
                       {:coll :line-item
                        :vlicGtid (-> line-item last :vlicGtid)
                        :no 1
                        :part-order-old [2
                                         {:coll :part-order
                                          :vlicGtid (-> part-order2 last :vlicGtid)
                                          :gaga "bobo"
                                          :order [1 {:coll :order :name :foo  :vlicGtid (-> order last :vlicGtid)}]
                                          :type [1 {:coll :type :vlicGtid (-> type last :vlicGtid)}]}]
                        :part-order-original [1
                                              {:coll :part-order
                                               :vlicGtid (-> part-order1 last :vlicGtid)
                                               :gaga "baba"
                                               :order [1 {:coll :order :name :foo :vlicGtid (-> order last :vlicGtid)
                                                          }]
                                               :type [1 {:coll :type :vlicGtid (-> type last :vlicGtid)}]}]}])))

(facts "facts about adding constraints (RICs) at runtime"
       (let [ctx (create-context meta-model)
             tx (create-tx ctx)]
         (dosync
          (insert tx :order 1 {:name :foo})
          (insert tx :order 2 {:name :foo2})
          (insert tx :part-order 1 {:order 1})
          (insert tx :part-order 2 {:order 2})
          (insert tx :part-order 3 {:order 2}))

         (fact "selecting by index before adding the ric that implies the index must fail"
               (select tx :part-order [:order] >= [2]) => (throws ConstraintException "Lambdaroyal-Memory No applicable index defined for key [:order] on collection [:part-order]"))

         (add-ric ctx {:name :part-order->order :coll :part-order :foreign-coll :order :foreign-key :order})
         (fact "adding as per constraint must be ok"
               (dosync (insert tx :part-order 20 {:order 2})))
         (fact 
          "frauding the dynamically added constraint must fail"
          (dosync
           (insert tx :part-order 21 {:order 3})) => (throws ConstraintException))
         (fact "selecting by index after adding the ric that implies the index must succed"
               (distinct
                (map 
                 #(-> % last :order) 
                 (select tx :part-order [:order] >= [2]))) => [2])))

(facts "check for unique gtid"
       (let [x (atom nil)
             y (atom nil)]
         (gtid-dosync 
          (reset! x @gtid))
         (gtid-dosync
          (reset! y @gtid))
         (fact "must be the same for nested tx" @x => (dec @y)))
       (let [x (atom nil)
             y (atom nil)]
         (gtid-dosync
          (gtid-dosync
           (reset! x @gtid))
          (gtid-dosync
           (reset! y @gtid)))
         (fact "must be the same for nested tx" @x => @y)))

(facts "facts about adding constraints (RICs) at runtime"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)]
    (dosync
     (let [x (insert tx :order 1 {:name :foo})
           y (insert tx :order 2 {:name :foo2})]
       (fact "*gtid* must not be bound" (bound? #'*gtid*) => falsey)
       (fact "if no derived dosync is used then the gtid_ must be set" (contains? (last x) :vlicGtid) => truthy)
       (fact "gtid of first inserted object must be below gtid of most recently inserted object" (< (-> x last :vlicGtid) (-> y last :vlicGtid)) => true)
       (fact "collection must contain gtid that matches the most recently used gtid" (-> ctx deref :order :gtid deref) => (-> y last :vlicGtid))))

    (gtid-dosync
     (let [x (insert tx :order 3 {:name :foo})
           y (insert tx :order 4 {:name :foo2})
           gtid' (-> x last :vlicGtid)]
       (fact "*gtid* must be same with object's gtid" gtid' => *gtid*)
       (fact "*gtid* must not be bound" (bound? #'*gtid*) => true?)
       (fact "collection does contain mru gtid" (-> ctx deref :order :gtid deref) => gtid')
       (let [coll (get @ctx :order)]
         (fact "gtid of collection must match most recently used gtid of object"
           (-> coll :gtid deref) => (-> y last :vlicGtid)))))))
















