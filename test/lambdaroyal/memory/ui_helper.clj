(ns lambdaroyal.memory.ui-helper
  (:gen-class))

(defn string-to-string [s]
  (if (empty? s) nil (.trim s)))

(defn string-to-int [s]
  (cond (nil? s) nil
        (= "" s) nil
        :else (java.lang.Integer/parseInt (.trim s))))

(defn string-to-long [s]
  (cond (nil? s) nil
        (= "" s) nil
        :else (java.lang.Long/parseLong (.trim s))))

(defn string-to-float [s]
  (cond (nil? s) nil
        (= "" s) nil
        :else (java.lang.Float/parseFloat (.trim s))))

(defn seq-to-ust [xs assocs]
  [(string-to-long (first xs))
   (let [keyvals (map 
                  (fn [x [key λ]]
                    [key (λ x)])
                  (rest xs)
                  assocs)]
     (zipmap (map first keyvals) (map last keyvals)))])

(defn slurp-csv [file & assocs]
  (let [assocs (or assocs [])
        keys (take-nth 2 assocs)
        fns (take-nth 2 (rest assocs))
        assocs (map
                (fn [key λ]
                  [(keyword key) λ])
                keys fns)]
    (with-open [rdr (clojure.java.io/reader file)]
      (doall
       (map
        (fn [line]
          (seq-to-ust (clojure.string/split line #";") assocs))
        (rest (line-seq rdr)))))))








