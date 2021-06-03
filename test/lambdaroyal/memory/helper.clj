(ns lambdaroyal.memory.helper
  
  (:require [clojure.tools.logging :as log]
            [monger.collection :as mc])
  (:gen-class))

(def log-count (atom 0))

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

(defmacro futures [n & exprs]
  (vec (for [_ (range n)
             expr exprs]
         `(future ~expr))))

(defmacro wait-futures [& args]
  `(doseq [f# (futures ~@args)] @f#))

(defmacro log-info-timed [msg form]
  (let [c (gensym "c")
        _ (gensym "_")
        t (gensym "t")]
    `(let [~c (swap! log-count inc)
           ~_ (log/info (format "[START %d] %s" ~c ~msg))
           ~t (timed ~form)
           ~_ (log/info (format "[STOP %d (ms) %f] %s" ~c (first ~t) ~msg))]
       (last ~t))))

(defn check-for-existence [db coll attr value timeout]
  (let [start-time (System/currentTimeMillis)]
       (loop []
         (if-let [doc (first (mc/find-maps db coll {attr value}))]
           doc
           (if (>= (- (System/currentTimeMillis)  start-time) timeout)
             (throw (IllegalStateException. "Too slow"))
             (recur))))))







