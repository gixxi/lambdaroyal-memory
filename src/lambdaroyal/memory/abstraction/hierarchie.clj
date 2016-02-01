(ns ^{:doc "totally renewed hierarchie builder"} 
  lambdaroyal.memory.abstraction.hierarchie)


;; --------------------------------------------------------------------
;; BUILDING A DATA HIERARCHIE
;; CR - https://github.com/gixxi/lambdaroyal-memory/issues/1
;; This allows to build a hierarchie of documents as per hierarchie 
;; levels. A hierarchie level denotes an attribute of the document - 
;; either directly (first level attribute) or indirectly by applying 
;; a function to the document
;; --------------------------------------------------------------------

(defn- flat-nested 
  "check whether we have stacked sequences due to missing levels (flat them)"
  [xs]
  (loop [xs xs]
    (if
        (or (-> xs sequential? not) (> (count xs) 1)
            (-> xs first sequential? not))
      xs
      (recur (first xs)))))

(defn- wrap-up [xs]
(if (sequential? xs)
                     xs
                     (cons xs '())))

(defn hierarchie-backtracking-int
  [rec xs handler λbacktracking & levels]
  (if levels
    (let [level (first levels)
          next (rest levels)
          xs' (group-by #(level %) xs)
          ;; we have three cases, 
          ;; (1) all x of xs have the level discriminator, we just proceed recursivly
          ;; (2) a true subset of xs have the the level discriminator, we process recursivly but assoc nil to key :k to the nodes that do not have the level discriminator
          ;; (3) none of xs have the level discriminator, we skipt the level
          key# (keys xs')
          nil# (filter nil? key#)
          xs'' (if 
                   ;;(3) none of xs have the level discriminator, we skipt the level
                   (= key# nil#)
                 (wrap-up 
                  (apply hierarchie-backtracking-int (inc rec) xs handler λbacktracking next))
                 
                 ;;else (1) and (2)
                 (map 
                  (fn [[k v]]
                    ;;atomic denotes whether a seq with just one element was present -> only the single element is returned
                    (let [res-from-recursion (wrap-up 
                                              (apply hierarchie-backtracking-int (inc rec) v handler λbacktracking next))]
                      ;;here we check whether we faced the leaf, just use the leaf data here
                      ;;->sounds strage, this is not yet e=mc²
                      (if 
                          (and 
                           (= (count res-from-recursion) 1)
                           (-> res-from-recursion first :l true?))
                        (merge (first res-from-recursion) {:r rec :k k :c 1})
                        ;;else
                        (λbacktracking                       
                         {:r rec :k k :l false :c (count v) :v res-from-recursion}))))
                  (map
                   (fn [k] [k (get xs' k)])
                   (-> xs' keys sort))))
          xs'' xs'']
      xs'')
    ;;else - no (more) levels -> get out user scope data wrapped up
    (λbacktracking {:r rec :l true :v (if handler (handler xs) xs)})))

(defn hierarchie-backtracking
  "[level] is variable arity set of keywords or function taking a document into account and providing back a category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs. 
  λbacktracking-fn must accept a map [node] that denotes the group key [:k], :l (denoting whether we inspect a leaf node or not and :v, the elements that resulted from applying a level discriminator, :c the number of elements WITHIN the next recursion matching the category. The function must return a adapted version of [node] that reflects the information necessary to the user."
  [xs handler λbacktracking & levels]
  (let [res
        (apply hierarchie-backtracking-int 0 xs handler λbacktracking levels)]
    (if (and (map? res) (-> res :l true?))
      (cons res '())
      res)))

(defn hierarchie
  "[level] is variable arity set of keywords or function taking a document into account and providing back a category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs."
  [xs handler & levels]
  (let [res (apply hierarchie-backtracking-int 0 xs handler identity levels)]
    (if (and (map? res) (-> res :l true?))
      (cons res '())
      res)))















