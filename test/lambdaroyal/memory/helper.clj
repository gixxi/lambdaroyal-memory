(ns lambdaroyal.memory.helper
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
