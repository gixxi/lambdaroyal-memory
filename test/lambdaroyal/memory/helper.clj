(ns lambdaroyal.memory.helper
  (:require [clojure.tools.logging :as log])
  (:import [java.text SimpleDateFormat])
  (:gen-class))

(def log-count (atom 0))

(defn append-to-timeseries [name & values]
  (if (not= "false" (System/getenv "lambdaroyal.memory.traceteststats.disable"))
    (let [dir (or (System/getenv "lambdaroyal.memory.traceteststats.dir") "test/stats/")
          filename (str dir name ".dat")
          format (SimpleDateFormat. "yyyy-MM-dd HH:mm:ss")]
      (with-open [w (clojure.java.io/writer filename :append true)]
        (.write w (apply str (.format format (new java.util.Date)) \; values))
        (.write w "\n")))))

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









