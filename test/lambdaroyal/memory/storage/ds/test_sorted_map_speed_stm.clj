(ns lambdaroyal.memory.storage.ds.test-sorted-map-speed-stm
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.helper :refer :all])
  (:gen-class))

(def ^:const num-cpu 4)
(def ^:const len 100000)

(defn warm-up [db partition]
  (doseq [p partition]
    (dosync
     (alter db assoc p (hash p)))))

(facts "test performance of sorted-map in a std setup with 100000 items"
  (let [db (ref (sorted-map))
        input-data (partition num-cpu (shuffle (range len)))
        delay (-> (reduce (fn [acc i] @i) nil (map #(future (warm-up db %)) input-data)) timed first)]
    (fact 
        "warm-up by 4 threads"
      delay => (roughly 0 4000))
    (fact "after warm-up all items need to be within db" (-> db deref rseq first first) => (dec len))))














