(ns lambdaroyal.memory.storage.ds.test-sorted-map-speed
  (:gen-class))

(defmacro timed [form]
  (let [start (gensym "start")
        obj (gensym "obj")
        delay (gensym "delay")]
    `(let [~start (. java.lang.System (clojure.core/nanoTime))
           ~obj ~form
           ~delay (clojure.core//
                   (clojure.core/double 
                    (clojure.core/- (. java.lang.System (clojure.core/nanoTime)) 
                                    ~start)) 1000000.0)]
       [~delay ~obj])))

(defn perf-spec [len]
  (let [raw-data-keys (shuffle (range len))
        raw-data-vals (map hash raw-data-keys)]
    (timed (reduce (fn [acc i]
                     (assoc acc i (hash i)))
                   (sorted-map)
                   raw-data-keys))))










