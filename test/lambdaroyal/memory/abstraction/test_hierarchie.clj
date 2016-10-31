(ns lambdaroyal.memory.abstraction.test-hierarchie
  (:require [midje.sweet :refer :all]
           [lambdaroyal.memory.abstraction.hierarchie :refer :all]
           [clojure.test :refer :all])
  (:import  [lambdaroyal.memory.core ConstraintException]))

(def stocks {16497 [[0 {:amount 1, :_id "0", :checkdigit1 "6667", :_rev "4-ad877d2c92613412f63b19195d6beade", :checkdigit0 "1111", :unique-key 0, :initialSidLabel "vlic.printable.1452905471675.928029181.tmp.pdf", :article 1, :location 16497}]], 17180 [[1 {:amount 1, :_id "1", :checkdigit1 "5566", :_rev "3-3aaa707d72ffe8b7e0cdf7c413d3896b", :checkdigit0 "2224", :unique-key 1, :initialSidLabel "vlic.printable.1452905486977.173646647.tmp.pdf", :article 1, :location 17180}]], 17161 [[2 {:amount 1, :_id "2", :checkdigit1 "6687", :_rev "3-2b684900be0a665b20244576121638c5", :checkdigit0 "1111", :unique-key 2, :initialSidLabel "vlic.printable.1452905488225.707374124.tmp.pdf", :article 1, :location 17161}]], nil [[3 {:_id "3", :_rev "1-152415a3fe52c0aa1604c9db61fc352e", :article 1, :amount 1, :checkdigit0 "2224", :checkdigit1 "5786", :unique-key 3}] [4 {:_id "4", :_rev "1-f62abc0250858328000ca211ec2b86b8", :article 1, :amount 1, :checkdigit0 "1111", :checkdigit1 "7767", :unique-key 4}] [5 {:_id "5", :_rev "1-10f22d116f756bea6e174b86ac644f63", :article 1, :amount 1, :checkdigit0 "2224", :checkdigit1 "6666", :unique-key 5}] [6 {:_id "6", :_rev "1-9ffa7e6c4dbe3c673704266c7f7cca6b", :article 1, :amount 1, :checkdigit0 "1114", :checkdigit1 "5787", :unique-key 6}] [7 {:_id "7", :_rev "1-30b369eb9b9dcc4c0094a640b23d82f5", :article 1, :amount 1, :checkdigit0 "2234", :checkdigit1 "7886", :unique-key 7}] [8 {:_id "8", :_rev "1-f83e19bfdca80ca2f01a88ad28aa6597", :article 1, :amount 1, :checkdigit0 "1343", :checkdigit1 "8867", :unique-key 8}] [9 {:_id "9", :_rev "1-7d024eab9d30b0e0c4846ce9e7315adc", :article 1, :amount 1, :checkdigit0 "4214", :checkdigit1 "6766", :unique-key 9}]]})

(def locations '([16316 {:_id "16316", :_rev "1-aed26191ff6ddfa05781f19111d30a66", :zone "WE", :capacity 999, :checkdigit0 "2121", :checkdigit1 "5756", :unique-key 16316}] [16317 {:_id "16317", :_rev "1-ece1d993c6aa9d4c3df8811eaf67e0ed", :zone "WA", :capacity 999, :checkdigit0 "4334", :checkdigit1 "7677", :unique-key 16317}] [16318 {:_id "16318", :checkdigit1 "6775", :_rev "1-6f631bb4542ba5f26e1150eb9f87d21a", :checkdigit0 "1242", :shelf 1, :place 1, :capacity 1, :zone "LEG", :unique-key 16318, :level 1}] [16319 {:_id "16319", :checkdigit1 "8858", :_rev "1-cdcf898aca07037bf6f8bb9f386b8f83", :checkdigit0 "3123", :shelf 1, :place 1, :capacity 1, :zone "LEG", :unique-key 16319, :level 2}] [16320 {:_id "16320", :checkdigit1 "5887", :_rev "1-e56ee32a2f1f16e402023c55fe3d5087", :checkdigit0 "1333", :shelf 1, :capacity 1, :zone "LEG", :unique-key 16320, :level 3}] [16321 {:_id "16321", :checkdigit1 "6586", :_rev "1-a383c42d147b08671200ba0acf4ad6a2", :checkdigit0 "4223", :shelf 1, :place 2, :capacity 1, :zone "LEG", :unique-key 16321, :level 1}] [16322 {:_id "16322", :checkdigit1 "6567", :_rev "1-02296f534649d39f3491fd012a90de26", :checkdigit0 "2133", :shelf 1, :place 2, :capacity 1, :zone "LEG", :unique-key 16322, :level 2}] [16323 {:_id "16323", :checkdigit1 "7678", :_rev "1-45610660ee2e96a16da3d25ed3971304", :checkdigit0 "4423", :shelf 1, :place 2, :capacity 1, :zone "LEG", :unique-key 16323, :level 3}] [17931 {:_id "17931", :_rev "1-e110b223b8b7918a294608c7b86f8612", :zone "BOG", :shelf 12, :capacity 28, :checkdigit0 "1311", :checkdigit1 "7775", :unique-key 17931}] [17930 {:_id "17930", :_rev "1-8d756499187ee4d67fd90f6eb8449f81", :zone "BOG", :shelf 11, :capacity 12, :checkdigit0 "4431", :checkdigit1 "8677", :unique-key 17930}] [17929 {:_id "17929", :_rev "1-6ca6fbdb357eaa9c1c97ebeecea84fce", :zone "BOG", :shelf 10, :capacity 18, :checkdigit0 "2141", :checkdigit1 "6756", :unique-key 17929}] [17928 {:_id "17928", :_rev "1-21c063566159e2d60a354b10d72ca2cc", :zone "BEG", :shelf 9, :capacity 18, :checkdigit0 "4221", :checkdigit1 "6555", :unique-key 17928}] [17927 {:_id "17927", :_rev "1-2e8d7b40af60f512484d316a67b83450", :zone "BEG", :shelf 8, :capacity 84, :checkdigit0 "1334", :checkdigit1 "6556", :unique-key 17927}] [17926 {:_id "17926", :_rev "1-2e77bfe524d9025895769149380acc1d", :zone "BEG", :shelf 7, :capacity 250, :checkdigit0 "4414", :checkdigit1 "8658", :unique-key 17926}] [17925 {:_id "17925", :_rev "1-e09a19fb9b01519247c9ef7dff9fd2e8", :zone "BEG", :shelf 6, :capacity 28, :checkdigit0 "3212", :checkdigit1 "5686", :unique-key 17925}] [17924 {:_id "17924", :_rev "1-7aa254464c89b6a6f2b0f3e877c6e50a", :zone "BEG", :shelf 5, :capacity 28, :checkdigit0 "2341", :checkdigit1 "7785", :unique-key 17924}] [17923 {:_id "17923", :_rev "1-0e8623c2378ee61a4d192fb3dba95c4a", :zone "BEG", :shelf 4, :capacity 16, :checkdigit0 "3413", :checkdigit1 "5787", :unique-key 17923}] [17922 {:_id "17922", :_rev "1-f90f3d74ca5ef1b36abe79278832f055", :zone "BEG", :shelf 3, :capacity 16, :checkdigit0 "1231", :checkdigit1 "7758", :unique-key 17922}]))

(def joined (map (fn [[k v]] [k (assoc v 
                                  :count-stock (if-let [stocks (get stocks k)] 
                                                 (count stocks) 0)
                                  :count-matching-stocks 0)]) locations))


(defn shortpath [x]
  #(-> % last x))

(defn hierarchie-location-for-inbound' [join]
  (apply hierarchie-backtracking join

         ;;handler-fn mapping the a sequence of leaves
         (fn [xs]
           (reduce
            (fn [acc x]
              (let [v (last x)
                    acc (assoc acc :locked (and (-> acc :locked nil?) (contains? v :locked)))                      
                    acc (assoc ( ;;check the current location has some space left
                                if (or (= 0 (:count-stock v))
                                       (and (:capacity v) (> (:capacity (:count-stock v)))))
                                 
                                 (assoc 
                                     acc                                     
                                   :capacity 
                                   (+ 
                                    (:capacity acc) 
                                    (if (:capacity v) 
                                      (- (:capacity v) (:count-stock v))
                                      1))
                                   :keys (conj
                                          (:keys acc) (first x)))
                                 acc)
                          :count-matching-stocks
                          (+ (:count-matching-stocks acc) (:count-matching-stocks v)))] acc))
            {:keys []
             :capacity 0 :count-matching-stocks 0}
            xs))

         ;;λbacktracking - aggregating capacity all the way up to the roots         
         (fn [node]
           (do 
             (if
                 (:l node)
               (assoc node :capacity (-> node :v :capacity))
               (try
                 (assoc node :capacity (apply + (map :capacity (:v node))))
                 (catch Exception e (throw (Exception. (format "Failure in backtracking on node %s" node) e))))
               ))) 
         (map #(shortpath %) [:zone :shelf :place :level])))

(defn hierarchie-location-for-inbound'' [join]
  (apply hierarchie-backtracking join

         ;;handler-fn mapping the a sequence of leaves
         (fn [xs]
           (reduce
            (fn [acc x]
              (let [v (last x)
                    acc (assoc acc :locked (and (-> acc :locked nil?) (contains? v :locked)))                      
                    acc (assoc ( ;;check the current location has some space left
                                if (or (= 0 (:count-stock v))
                                       (and (:capacity v) (> (:capacity (:count-stock v)))))
                                 
                                 (assoc 
                                     acc                                     
                                   :capacity 
                                   (+ 
                                    (:capacity acc) 
                                    (if (:capacity v) 
                                      (- (:capacity v) (:count-stock v))
                                      1))
                                   :keys (conj
                                          (:keys acc) (first x)))
                                 acc)
                          :count-matching-stocks
                          (+ (:count-matching-stocks acc) (:count-matching-stocks v)))] acc))
            {:keys []
             :capacity 0 :count-matching-stocks 0}
            xs))
         identity 
         (map #(shortpath %) [:zone :shelf :place :level])))

(hierarchie-location-for-inbound' joined)
(hierarchie-location-for-inbound'' joined)

(comment '([["WE" 1 999] {:keys [16316], :capacity 999, :count-matching-stocks 0, :locked false}]
           [["WA" 1 999] {:keys [16317], :capacity 999, :count-matching-stocks 0, :locked false}]
           [["LEG" 6 6]
            [[1
              6
              ([[1 1 1] {:keys [16318], :capacity 1, :count-matching-stocks 0, :locked false}]
               [[2 1 1] {:keys [16319], :capacity 1, :count-matching-stocks 0, :locked false}]
               [[3 1 1] {:keys [16320], :capacity 1, :count-matching-stocks 0, :locked false}])]
             ([[1 3 {:keys [16318], :capacity 1, :count-matching-stocks 0, :locked false}]
               ([[1 1 1] {:keys [16318], :capacity 1, :count-matching-stocks 0, :locked false}]
                [[2 1 1] {:keys [16319], :capacity 1, :count-matching-stocks 0, :locked false}]
                [[3 1 1] {:keys [16320], :capacity 1, :count-matching-stocks 0, :locked false}])]
              [[2 3 {:keys [16321], :capacity 1, :count-matching-stocks 0, :locked false}]
               ([[1 1 1] {:keys [16321], :capacity 1, :count-matching-stocks 0, :locked false}]
                [[2 1 1] {:keys [16322], :capacity 1, :count-matching-stocks 0, :locked false}]
                [[3 1 1] {:keys [16323], :capacity 1, :count-matching-stocks 0, :locked false}])])]]
           [["BOG" 3 {:keys [17931], :capacity 28, :count-matching-stocks 0, :locked false}]
            ([[12 1 28] {:keys [17931], :capacity 28, :count-matching-stocks 0, :locked false}]
             [[11 1 12] {:keys [17930], :capacity 12, :count-matching-stocks 0, :locked false}]
             [[10 1 18] {:keys [17929], :capacity 18, :count-matching-stocks 0, :locked false}])]
           [["BEG" 7 {:keys [17928], :capacity 18, :count-matching-stocks 0, :locked false}]
            ([[9 1 18] {:keys [17928], :capacity 18, :count-matching-stocks 0, :locked false}]
             [[8 1 84] {:keys [17927], :capacity 84, :count-matching-stocks 0, :locked false}]
             [[7 1 250] {:keys [17926], :capacity 250, :count-matching-stocks 0, :locked false}]
             [[6 1 28] {:keys [17925], :capacity 28, :count-matching-stocks 0, :locked false}]
             [[5 1 28] {:keys [17924], :capacity 28, :count-matching-stocks 0, :locked false}]
             [[4 1 16] {:keys [17923], :capacity 16, :count-matching-stocks 0, :locked false}]
             [[3 1 16] {:keys [17922], :capacity 16, :count-matching-stocks 0, :locked false}])]))
