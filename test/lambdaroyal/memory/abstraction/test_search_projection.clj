(ns 
    ^{:doc "(Performance) Unittests for lambdaroyal memory search abstraction that builds data projections."
      :author "christian.meichsner@live.com"}
    lambdaroyal.memory.abstraction.test-search-projection
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.abstraction.search :refer :all]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [clojure.core.async :refer [>! alts!! timeout chan go]])
  (:import [java.text SimpleDateFormat]))

(defn append-to-timeseries [name & values]
  (if (not= "false" (System/getenv "lambdaroyal.memory.traceteststats.disable"))
    (let [dir (or (System/getenv "lambdaroyal.memory.traceteststats.dir") "test/stats/")
          filename (str dir name ".dat")
          format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
      (with-open [w (clojure.java.io/writer filename :append true)]
        (.write w (apply str (.format format (new java.util.Date)) \; values))
        (.write w "\n")))))

;;we try to be idempotent, so we don't use mutable models from other workspaces here
(def ^:const threads 10)

(def meta-model
  {:client {:indexes []} 
   :type {:indexes []}
   :article {}
   :line-item {:foreign-key-constraints [{:foreign-coll :article :foreign-key :art1}
                                         {:foreign-coll :article :foreign-key :art2}]}
   :order {:indexes [{:unique true :attributes [:name]}]
           :foreign-key-constraints [{:foreign-coll :client :foreign-key :client}]}
   :part-order {:indexes [] 
                :foreign-key-constraints [{:name :type :foreign-coll :type :foreign-key :type}
                                          {:name :order :foreign-coll :order :foreign-key :order}]}})



(let [articles '(:apple :banana :avocado)        
             ctx (create-context meta-model)
             tx (create-tx ctx)
             bulk (timed (dosync
                          (doseq [[k v] (zipmap (range) articles)]
                            (insert tx :article k {:name v}))
                          (let [apple (select-first tx :article 0)
                                banana (select-first tx :article 1)
                                avocado (select-first tx :article 2)
                                line-items [{:name :justapple :art1 (first apple) :art2 (first avocado)}
                                            {:name :justbanana :art1 (first banana) :art2 (first banana)}
                                            {:name :appleandbanana :art1 (first banana) :art2 (first apple)}]]
                            (doseq [[k v] (zipmap (range) line-items)]
                              (insert tx :line-item k v))
                            (doseq [[k v] (zipmap (range (count line-items) (* 2 (count line-items))) line-items)]
                              (insert tx :line-item k v)))))
             _ (println "insert took (ms) " (first bulk))
             apple (select-first tx :article 0)
             banana (select-first tx :article 1)
             avocado (select-first tx :article 2)]
         

         (let [apple-line-items (take 2 (filter #(= (first apple) (-> % last :art1)) (select tx :line-item)))
               _ (doseq [x apple-line-items] (println x))
               banana-line-items (take 3 (filter #(= (first banana) (-> % last :art1)) (select tx :line-item)))
               _ (doseq [x banana-line-items] (println x))
               xs (concat banana-line-items apple-line-items)
               proj (by-referencees tx :line-item :article xs :verbose true)
               proj' (proj tx (filter-xs :line-item xs)
                           (<<< :article :verbose true))]
           (fact "by-referenees returns distinct referenced cards of article master-data " proj => [banana apple])
           (fact "<< returns distinct referenced cards of article master-data " proj' => [banana apple])
           proj))


(facts "check whether projections on non-unique collection associations (collection a referres collection b by two distinct rics"
       (let [articles '(:apple :banana :avocado)        
             ctx (create-context meta-model)
             tx (create-tx ctx)
             bulk (timed (dosync
                          (doseq [[k v] (zipmap (range) articles)]
                            (insert tx :article k {:name v}))
                          (let [apple (select-first tx :article 0)
                                banana (select-first tx :article 1)
                                avocado (select-first tx :article 2)
                                line-items [{:name :justapple :art1 (first apple) :art2 (first avocado)}
                                            {:name :justbanana :art1 (first banana) :art2 (first banana)}
                                            {:name :appleandbanana :art1 (first banana) :art2 (first apple)}]]
                            (doseq [[k v] (zipmap (range) line-items)]
                              (insert tx :line-item k v)))))
             _ (println "insert took (ms) " (first bulk))
             apple (select-first tx :article 0)
             banana (select-first tx :article 1)
             avocado (select-first tx :article 2)]
         (let [ric (ric tx :line-item :article)]
           (fact "ric is present" ric =>  truthy))
         (let [ric (ric tx :line-item :article :art1)]
           (fact "ric is present by specific foreign-key" ric =>  truthy)
           (fact "ric denotes proper foreign-key" (.foreign-key ric) => :art1))
         (let [ric (ric tx :line-item :article :art2)]
           (fact "ric is present by specific foreign-key" ric =>  truthy)
           (fact "ric denotes proper foreign-key" (.foreign-key ric) => :art2))
         (let [ric (ric tx :line-item :article :art3)]
           (fact "ric is present by specific foreign-key" ric =>  falsey))
         (let [proj (by-ric tx :line-item :article [(first apple)] :foreign-key :art1 :verbose true)]
           (fact "specific ric must reveal correct number of items" (count proj) => 1))

         (let [apple-line-items (take 2 (filter #(= (first apple) (-> % last :article)) (select tx :line-item)))
               banana-line-items (take 3 (filter #(= (first apple) (-> % last :article)) (select tx :line-item)))
               proj (by-referencees tx :line-item :article (concat banana-line-items apple-line-items) :verbose true)
               proj' (proj tx (filter-xs :line-item (concat apple-line-items banana-line-items))
                           (<<< :article :verbose true))]
           (fact "by-reference must reveal a set of articles without redundancies"
                 proj => [banana apple])
           (fact "<<< must reveal a set of articles without redundancies"
                 proj' => [banana apple]))

         (let [proj (by-ric tx :line-item :article [(first apple) (first banana)] :foreign-key :art1 :verbose true)]
           (fact "specific ric must reveal correct number of items" (count proj) => 3))
         ;;now the more handsome version
         (let [proj (time (proj tx
                                (filter-key tx :article (first apple))
                                (>>> :line-item :foreign-key :art1 :verbose true)))]
           (fact "project on specific ric #1 works" (map #(-> % last :name) proj) => [:justapple]))
         (let [proj (time (proj tx
                                (filter-key tx :article (first apple))
                                (>>> :line-item :foreign-key :art1 :verbose true :reverse true)))]
           
           (fact "project on specific ric #1 works" (map #(-> % last :name) proj) => [:justapple]))

         (let [proj (time (proj tx
                                (filter-key tx :article (first apple))
                                (>>> :line-item :foreign-key :art2 :verbose true)))]
           (fact "project on specific ric #2 works" (map #(-> % last :name) proj) => [:appleandbanana]))
         (let [proj (time (proj tx
                                (filter-key tx :article (first banana))
                                (>>> :line-item :foreign-key :art2 :verbose true)))]
           (fact "project on specific ric #2 works" (map #(-> % last :name) proj) => [:justbanana]))
         (let [proj (time (proj tx
                                (filter-key tx :article (first banana))
                                (>>> :line-item :foreign-key :art1 :verbose true)))]
           (fact "project on specific ric #2 works" (into #{} (map #(-> % last :name) proj)) => #{:justbanana :appleandbanana}))))

(facts "inserting 300 Orders, each with 4-6 partOrders, each partorder with 10-20 lineitems -> 75000 line items"
       (let [types '(:post :express :pick-up :store)
             clients '(:europe :africa :asia)
             targets (range 10)
             deliverers '(:fedex :dhl :post)
             articles '(:banana :apple :peach :plum)
             batches '(:old :new :smelling)
             poid (atom 0)
             oid (atom 0)
             liid (atom 0)
             ctx (create-context meta-model)
             tx (create-tx ctx)
             bulk (timed
                   (dosync
                    (doseq [[k v] (zipmap (range) types)]
                      (insert tx :type k {:name v}))
                    (doseq [[k v] (zipmap (range) clients)]
                      (insert tx :client k {:name v})
                      (doseq [x (range 100)]
                        (insert tx :order (swap! oid inc) {:name (str v x) :client k :deliverer (rand-nth deliverers)})
                        (doseq [y (repeatedly (+ 4 (rand-int 3)) #(swap! poid inc))]
                          (insert tx :part-order y {:order-no (str v x) :order @oid :type (-> types count rand-int) :target (rand-nth targets) :client k}))))))
             _ (println "insert took (ms) " (first bulk))
             _ (println :clients (select tx :client))]
         (let [ric (ric tx :order :client)]
           (fact "order->client" ric =>  truthy)
           (fact "order->client" (.foreign-coll ric) => :client))
         (let [ric (ric tx :part-order :type)]
           (fact "part-order->type" ric =>  truthy)
           (fact "part-order->type" (.foreign-coll ric) => :type))
         (let [proj (by-ric tx :order :client [0])]
           (fact "client->order for one key" (count proj) => 100)
           (fact "client->order for one key" (distinct (map #(-> % last :client) proj)) => [0]))
         
         ;; TESTING WRAPPING UP ALL THE SEARCH LAMBDAS (filter-xxx by the pipe function that does the by-ric
         ;; HELL OF A WORK
         (let [proj ((>> :order (fn [x] true)) tx ((filter-key tx :client 1)))]
           (fact "client->order using the pipe for one key" (count proj) => 100)
           (fact "client->order using the pipe for one key" (distinct (map #(-> % last :client) proj)) => [1]))
         (let [proj ((>> :order (fn [x] true)) tx ((filter-key tx :client > 1)))]
           (fact "client->order using the pipe for key [2]" (count proj) => 100)
           (fact "client->order using the pipe for one key" (distinct (map #(-> % last :client) proj)) => [2]))
         (let [proj ((>>> :part-order) tx ((filter-index tx :order [:name] >= [":europe"])))]
           (fact "order->partorder using the pipe for key [:europe]" (count proj) => (roughly 100 500))
           (fact "client->order using the pipe for one key" (remove #(if (.startsWith % ":europe") %) (map #(-> % last :order-no) proj)) => empty))
         ;;the ugly style variant - but working
         (let [proj ((>>> :part-order) tx
                     ((>>> :order) tx ((filter-key tx :client 2))))]
           (fact "type->order->partorder using the pipe" (count proj) => (roughly 100 500))
           (fact "client->order using the pipe for one key" (remove #(if (= % 2) %) (map #(-> % last :client) proj)) => empty))
         ;;now the more handsome version
         (let [_ (println :proj)
               proj (time (proj tx
                                (filter-key tx :client 2)
                                (>>> :order)
                                (>>> :part-order)))]
           (fact "type->order->partorder using the pipe" (count proj) => (roughly 100 500))
           (fact "client->order using the pipe for one key" (remove #(if (= % 2) %) (map #(-> % last :client) proj)) => empty))

         (let [_ (println :proj)
               proj (time (proj tx
                                (filter-key tx :client 2)
                                (>>> :order)
                                (>>> :part-order)))]
           (loop [xs proj]
             (if (>= (count xs) 2)
               (let [[x y] (map first (take 2 xs))]
                 (fact "projecting on target tupels in ascending order must yield ascending order" (< x y) => truthy)
                 (recur (drop 2 xs))))))

         (let [_ (println :proj :descending)
               proj (time (proj tx
                                (filter-key tx :client 2)
                                (>>> :order)
                                (>>> :part-order :reverse true)))]
           (loop [xs proj]
             (if (>= (count xs) 2)
               (let [[x y] (map first (take 2 xs))]
                 (fact "projecting on target tupels in ascending order must yield ascending order" (> x y) => truthy)
                 (recur (drop 2 xs))))))


         ;;some speed test
         (let [[t _] (timed (apply + (map count 
                                          (pmap 
                                           #(proj tx
                                                  (filter-key tx :client %)
                                                  (>>> :order)
                                                  (>>> :part-order)) 
                                           (-> clients count range)))))]
           (append-to-timeseries "projection" (apply str (interpose ";" [t (float (/ (-> ctx deref :part-order :data deref count) 100))])))
           (fact "(parallel) 3x type->order->partorder using the pipe" t => (roughly 30 80)))


         (let [proj (by-ric tx :order :client [0 2])]
           (fact "client->order for one key" (count proj) => 200)
           (fact "client->order for one key" (distinct (map #(-> % last :client) proj)) => (just [2 0] :in-any-order)))
         (let [proj (timed (by-ric tx :order :client [0 1 2] :parallel false :ratio-full-scan 1.1))
               proj' (timed (by-ric tx :order :client [0 1 2] :parallel true :ratio-full-scan 1.1))
               _ (println "parallel vs seriell " (first proj) (first proj'))]
           (fact "client->order for one key" (-> proj last count) => 300)
           (fact "client->order for one key" (distinct (map #(-> % last :client) (last proj))) => [0 1 2]))
         (fact "all elements inserted" (-> ctx deref :order :data deref count) => 300)
         (fact "check quantities" (-> ctx deref :part-order :data deref count) => (roughly 50000 100000))
         (fact "check filter-all for projection"
               ((filter-all tx :client)) => (select tx :client))
         (fact "check filter-key for projection"
               ((filter-key tx :client 1)) => (conj [] (select-first tx :client 1)))
         (fact "check filter-key for projection with no-stop"
               ((filter-key tx :client >= 1)) => (select tx :client >= 1))
         (fact "check filter-index for projection with no stop"
               (count ((filter-index tx :order [:name] >= [":europe"]))) => (count (select tx :order [:name] >= [":europe"])))
         (fact "check filter-index for projection with range"
               (count ((filter-index tx :order [:name] >= [":asia"] <= [":europe"]))) => (count (select tx :order [:name] >= [":asia"] <= [":europe"])))))

(facts "test projection on empty target"
       (let [types '(:post :express :pick-up :store)
             clients '(:europe :africa :asia)
             targets (range 10)
             deliverers '(:fedex :dhl :post)
             articles '(:banana :apple :peach :plum)
             batches '(:old :new :smelling)
             poid (atom 0)
             oid (atom 0)
             liid (atom 0)
             ctx (create-context meta-model)
             tx (create-tx ctx)]
         (let [proj (by-ric tx :order :client [0])]
           (fact "client->order for one key" (count proj) => 0))
         
         ;; TESTING WRAPPING UP ALL THE SEARCH LAMBDAS (filter-xxx by the pipe function that does the by-ric
         ;; HELL OF A WORK
         (let [proj ((>> :order (fn [x] true)) tx ((filter-key tx :client 1)))]
           (fact "client->order using the pipe for one key" (count proj) => 0))
         
         (let [proj ((>>> :part-order) tx ((filter-index tx :order [:name] >= [":europe"])))]
           (fact "order->partorder using the pipe for key [:europe]" (count proj) => 0))
         ;;now the more handsome version
         (let [_ (println :proj)
               proj (time (proj tx
                                (filter-key tx :client 2)
                                (>>> :order)
                                (>>> :part-order)))]
           (fact "type->order->partorder using the pipe" (count proj) => 0))))


















