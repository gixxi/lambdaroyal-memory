(ns 
    ^{:doc "(Performance) Unittests for lambdaroyal memory search abstraction that builds data projections."
      :author "christian.meichsner@live.com"}
    lambdaroyal.memory.abstraction.test-reverse-search-projection
  (:require [midje.sweet :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.abstraction.search :as search]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.helper :refer :all]
            [clojure.core.async :refer [>! alts!! timeout chan go]])
  (:import [java.text SimpleDateFormat]))


(def meta-model
  {:client {:indexes []} 
   :type {:indexes []}
   :article {}   
   :stock {:foreign-key-constraints [{:foreign-coll :article :foreign-key :art}]}   
   :line-item {:foreign-key-constraints [{:foreign-coll :article :foreign-key :art1}
                                         {:foreign-coll :article :foreign-key :art2}]}
   :order {:indexes [{:unique true :attributes [:name]}]
           :foreign-key-constraints [{:foreign-coll :client :foreign-key :client}]}
   :part-order {:indexes [] 
                :foreign-key-constraints [{:name :type :foreign-coll :type :foreign-key :type}
                                          {:name :order :foreign-coll :order :foreign-key :order}]}})



(let [articles '(:apple :banana :avocado :melon)        
             ctx (create-context meta-model)
             tx (create-tx ctx)
             bulk (timed (dosync
                          (doseq [[k v] (zipmap (range) articles)]
                            (insert tx :article k {:name v}))
                          (let [apple (select-first tx :article 0)
                                banana (select-first tx :article 1)
                                avocado (select-first tx :article 2)                                
                                melon (select-first tx :article 3)
                                line-items [{:name :justapple :art1 (first apple) :art2 (first avocado)}
                                            {:name :justbanana :art1 (first banana) :art2 (first banana)}
                                            {:name :appleandbanana :art1 (first banana) :art2 (first apple)}]]
                            (doseq [[k v] (zipmap (range 10) (repeat {:art (first avocado) :batch "foo"}))]
                              (insert tx :stock k v))
                            (doseq [[k v] (zipmap (range 10 20) (repeat {:art (first melon) :batch "bar"}))]
                              (insert tx :stock k v))
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
               proj (search/by-referencees tx :line-item :article xs :verbose true)
               proj' (search/proj tx (search/filter-xs :line-item xs)
                           (search/<<< :article :verbose true))
               proj'' (search/proj tx (search/filter-xs :line-item xs)
                                  (search/<<< :article :foreign-key :art2 :verbose true))
               proj''' (search/proj tx (search/filter-xs :line-item xs)
                                  (search/<<< :article :foreign-key :art2 :verbose true)
                                  (search/>>> :stock :verbose true))
               proj'''' (search/proj tx (search/filter-xs :line-item xs)
                                  (search/<<< :article :foreign-key :art2 :verbose true)
                                  (search/>>> :stock :verbose true :reverse true))
               proj''''' (search/proj tx (search/filter-xs :line-item xs)
                           (search/<<< :article :verbose true :reverse true))
               expected (take-while #(= (first avocado) (-> % last :art)) (select tx :stock [:art] >= [(first avocado)]))
               _ (doseq [x proj''']
                   (println :xεproj''' x))
               _ (doseq [x proj'''']
                   (println :xεproj'''' x))
               _ (doseq [x expected]
                   (println :xεexpected x))]
           (fact "by-referenees returns distinct referenced cards of article master-data " proj => [banana apple])
           (fact "<<< returns distinct referenced cards of article master-data " proj' => [banana apple])
           (fact "<<< :reverse true returns distinct referenced cards of article master-data " proj''''' => [banana apple])
           (fact "<<< on alternative foreign-key returns distinct referenced cards of article master-data " proj'' => [banana apple avocado])
           (fact "<<< :article followed by >>> :stock reveals stock " proj''' => (sort-by first expected))
           (fact "<<< :article followed by >>> :stock :reverse true reveals reversed stock " proj'''' => (reverse (sort-by first expected)))))
