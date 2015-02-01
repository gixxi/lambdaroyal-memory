(ns lambdaroyal.memory.storage.ds.test-sorted-map-speed
  (require [midje.sweet :refer :all])
  (:gen-class))

(defmacro timed [form]
  "evaluates parameter form to obj and gives back the tuple (delay obj) where delay denotes the floating point number of ms needed to eval the form"
  (let [start (gensym "start")
        obj (gensym "obj")
        delay (gensym "delay")]
    `(let [~start (. java.lang.System (clojure.core/nanoTime))
           ~obj ~form
           ~delay (clojure.core//
                   (clojure.core/double 
                    (clojure.core/- (. java.lang.System (clojure.core/nanoTime)) 
                                    ~start)) 1000000.0)]
       (list ~delay ~obj))))

(defn perf-spec [len]
  (let [raw-data-keys (shuffle (range len))
        raw-data-vals (map hash raw-data-keys)]
    (let [warm-up (timed 
                   (reduce 
                    (fn [acc i]
                      (assoc acc i (hash i)))
                    (sorted-map)
                    raw-data-keys))]
      warm-up)))

(facts "test performance of sorted-map datastructure"
  (let [res10000 (perf-spec 10000)]
    (fact "warmup time" (first res10000)) => (< 10)))









