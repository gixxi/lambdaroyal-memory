(ns lambdaroyal.memory.storage.ds.test-sorted-map-speed
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.helper :refer :all])
  (:gen-class))

(defn perf-spec [len]
  (let [raw-data-keys (shuffle (range len))
        raw-data-vals (map hash raw-data-keys)]
    (let [warm-up (timed 
                   (reduce
                    (fn [acc i]
                      (assoc acc i (hash i)))
                    (sorted-map)
                    raw-data-keys))
          max (timed (-> warm-up last rseq first))
          find-helper (fn [a]
                        (val (find (-> warm-up last) (rand-int len))))
          find-subsequence-helper (fn []
                                    (let [start (- (rand-int (-> warm-up last count)) 100)]
                                      (map last (subseq (-> warm-up last) >= start < (+ start 100)))))
          find (timed
                (reduce + (map
                         find-helper
                         (range 10))))
          find-parallel (timed
                         (reduce + (pmap find-helper (range 1000))))
          subsequence (timed
                       (reduce
                        (fn [acc c]
                          (+ acc (apply + (find-subsequence-helper))))
                        0
                        (range 1000)))]      
      {:warm-up warm-up :max max :find find :find-parallel find-parallel :find-range subsequence})))

(let [res (perf-spec 10000)
      _ (println (map (fn [[k v]] (list k (first v))) res))] 
  (facts "test performance of sorted-map datastructure with 10000 items"
    (fact "warmup time" (-> res :warm-up first) => (roughly 0 50 ))
    (fact "max time" (-> res :max first) => (roughly 0 0.1 ))
    (fact "find time sequential" (-> res :find first) => (roughly 0 25))
    (fact "find time parallel" (-> res :find-parallel first) => (roughly 0 25))
    (fact "find time range - find 1000 times a range using the index" (-> res :find-range first) => (roughly 0 500))
))

(let [res (perf-spec 200000)
      _ (println (map (fn [[k v]] (list k (first v))) res))] 
  (facts "test performance of sorted-map datastructure with 200000 items"
    (fact "warmup time" (-> res :warm-up first) => (roughly 0 1500))
    (fact "max time" (-> res :max first) => (roughly 0 0.1 ))
    (fact "find time sequential" (-> res :find first) => (roughly 0 25))
    (fact "find time parallel - find 1000 times a range using the index" (-> res :find-parallel first) => (roughly 0 500))))






















