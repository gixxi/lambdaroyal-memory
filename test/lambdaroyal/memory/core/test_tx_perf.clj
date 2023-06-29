(ns 
    ^{:doc "Performance Unittests for lambdaroyal memory. result eviction to influxdb."
      :author "christian.meichsner@live.com"}
  lambdaroyal.memory.core.test-tx-perf
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.abstraction.search :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [clojure.core.async :refer [>! alts!! timeout chan go]]))

;;we try to be idempotent, so we don't use mutable models from other workspaces here

(def ^:const threads 10)

(def meta-model
  {:type
   {:indexes []}
   :order
   {:indexes [{:name :client :unique false :attributes [:client]}
                           {:name :client-no :unique false :attributes [:client :no]}]}
   :part-order
   {:indexes [] :foreign-key-constraints [
                                                       {:name :type :foreign-coll :type :foreign-key :type}
                                                       {:name :order :foreign-coll :order :foreign-key :order}]}
   :line-item
   {:indexes [] :foreign-key-constraints [{:name :part-order :foreign-coll :part-order :foreign-key :part-order}]}})

(defn- insert-future [ctx prefix]
  "spawns a thread that starts 100 consecutive transactions, each transaction consists of 10 inserts"
  (future
    (let [tx (create-tx ctx)]
      (doseq [p (partition 10 (range 1000))]
        (dosync
         (doseq [i p]
           (let [idx (+ (* 1000 prefix) i)]
             (insert tx :order idx {:client (rand-int 100)})
             (insert tx :part-order (format "%d:%s" idx "A") {:order idx :type 1 :sort :first})
             (insert tx :part-order (format "%d:%s" idx "B") {:order idx :type 1 :sort :first}))))))
    prefix))

(facts "check timing constraints inserting 10000 orders and 20000 partorders with 10 futures, each future executing 1000 transaction a 10+20 inserts"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync (insert tx :type 1 {}))
        bulk (timed
              (reduce (fn [acc i] (+ acc @i)) 0 (pmap #(insert-future ctx %) (range threads))))
        _ (println "test took (ms) " (first bulk))
        _ (append-to-timeseries "30000insertsBy10Threads" (first bulk))]
    (fact "all elements inserted" (-> ctx deref :order :data deref count) => (* 1000 threads))
    (fact "all threads finished" (float (last bulk)) => (Math/floor (/ (* threads (dec threads)) 2)))
    (fact "max time for 10000+20000 parallel inserts" (first bulk) => (roughly 0 15000))))

(defn wait [ms f & args]
  (let [c (chan)]
    (go (>! c (apply f args)))
    (first (alts!! [c (timeout ms)]))))

(defn shortpath [[x y]]
  [#(-> % last x) #(-> % last y)])

(facts "inserting 10³ Orders, each with 4-6 partOrders, each partorder with 10-20 lineitems -> 75000 line items"
  (let [modes '(:post :express :pick-up :store)
        targets (range 10)
        deliverers '(:fedex :dhl :post)
        articles '(:banana :apple :peach :plum)
        batches '(:old :new :smelling)
        poid (atom 0)
        liid (atom 0)
        ctx (create-context meta-model)
        tx (create-tx ctx)
        _ (dosync (insert tx :type 1 {}))
        bulk (timed
              (dosync
               (doseq [x (range 1000)]
                 (insert tx :order x {:mode (rand-nth modes) :deliverer (rand-nth deliverers)})
                 (doseq [y (repeatedly (+ 4 (rand-int 3)) #(swap! poid inc))]
                   (insert tx :part-order y {:order x :type 1 :target (rand-nth targets)})
                   (doseq [z (repeatedly (+ 10 (rand-int 10)) #(swap! liid inc))]
                     (insert tx :line-item z {:part-order y :amount (rand-int 1000) :article (rand-nth articles) :batch (rand-nth batches)}))))))
        _ (println "test took (ms) " (first bulk))
        lineitems (select tx :line-item >= 0)
        trees (timed (doall (map #(tree tx :line-item %) lineitems)))
        _ (println "build document trees took (ms) " (first trees))
        hierarchie-simple-count (timed 
                                 (hierarchie (last trees) count 
                                             #(-> % last :article)
                                             #(-> % last :batch)
                                             #(-> % last :part-order last :order last :deliverer)
                                             #(-> % last :part-order last :order last :mode)))
        _ (println "building a counting hierarchie :article :batch :deliverer :mode took (ms)" (first hierarchie-simple-count))

        hierarchie-simple-data (timed 
                                 (hierarchie (last trees) identity 
                                             #(-> % last :article)
                                             #(-> % last :batch)
                                             #(-> % last :part-order last :order last :deliverer)
                                             #(-> % last :part-order last :order last :mode)))
        _ (println "building a data hierarchie :article :batch :deliverer :mode took (ms)" (first hierarchie-simple-data))

        _ (append-to-timeseries "1000Orders" (apply str (interpose ";" [(first bulk) (first trees) (first hierarchie-simple-count) (first hierarchie-simple-data)])))
]
    (fact "all elements inserted" (-> ctx deref :order :data deref count) => 1000)
    (fact "check quantities" (-> ctx deref :line-item :data deref count) => (roughly 50000 100000))
    (fact "max time for sequential inserts" (first bulk) => (roughly 0 150000))))

















