(ns lambdaroyal.memory.abstraction.search
  (:require [clojure.core.typed :as t]
            [clojure.core.async :refer [>! <! alts! timeout chan go]]))

;; --------------------------------------------------------------------
;; TYPE ABSTRACTIONS
;; --------------------------------------------------------------------

(t/defalias TMap (t/Map t/Any t/Any))

(defn abstract-search 
  "higher order function - takes a function [fn] that returns a lazy sequence [s] of user-scope tuples from lambdaroyal memory. This function returns a function that returns a channel where the result of the function fn, s is pushed to"
  [λ]
  (fn [args]
    (let [c (chan)]
      (do
        (go (>! c (λ args)))
        c))))

(defn combined-search
  "higher order function - takes a sequence of concrete functions (abstract-search) [fns] a aggregator function [agr], a query parameter [query] (can be a sequence) and optional parameters [opts] and returns go block. all the search functions [fns] are called with parameter [query] executed in parallel and feed their result (a sequence of user-scope tuples from lambdaroyal memory) to the aggregator function [agr]. By default the resulting go routine waits for all fn in [fns] for delivering a result.

  The following options are accepted\n 
  :timeout value in ms after which the aggregator channel is closed, no more search results are considered.\n
  :minority-report number of search function fn in [fns] that need to result in order to close the aggregator channel is closed and no more search results are considered.\n
  :finish-callback a function with no params that gets called when the aggregator go block stops."
  [fns agr query & opts]
  (let [opts (apply hash-map opts)
        c (chan)
        t (if (:timeout opts) (timeout (:timeout opts)) nil)
        limit (or (:minority-report opts) (count fns))
        ;;jerk the jokers
        limit (min limit (count fns))]
    ;;fire of all searches in parallel
    (doseq [λ fns]
      (go (>! c (<! (λ query)))))
    ;;fire of the aggregator (controller) go block
    (go
      (loop [i 0 stop false]
        (if 
            ;;recur until we reach timeout or all searches returned
            (or 
             (>= i limit)
             stop)
          (if (:finish-callback opts) ((:finish-callback opts)))
          (let [next (if t 
                       ;;take result or bump timeout
                       (first (alts! [c t]))
                       ;;else : wait indef for result
                       (<! c))]
            ;;send most recent search result (next) to the aggregator
            (if next (agr next))
            ;;loop - wait for the next
            (recur (inc i) (nil? next))))))))

(defn concat-aggregator 
  "assumes ref to be a vector reference"
  [ref data]
  (dosync
   (commute ref concat data)))

(defn set-aggregator
  "assumes that data to be aggregated in are collection tupels where the document (second element) contains a key :coll denoting the collection the tuple belongs to. yields a set of collection tuples. assumes the ref to be a set"
  [ref data]
  (dosync
   (apply commute ref conj data)))

(defn gen-sorted-set 
  "generates a STM ref on a sorted set that can be used in conjunction with set-aggregator"
  []
  (ref (sorted-set-by
        (fn [x x']
          (let [coll (-> x last :coll)
                coll' (-> x' last :coll)]
            (if 
                (= coll coll')
              (compare (first x) (first x'))
              (compare coll coll')))))))

(defn sorted-set-aggregator
  "assumes that data to be aggregated in are collection tupels where the document (second element) contains a key :coll denoting the collection the tuple belongs to and all the keys (first element) are comparable with each other yields a set of collection tuples. assumes that ref denotes a sorted-set-by. the keys of the map are (collection tuple-key)"
  [ref data]
  (doseq [x data]
    (dosync
     (commute ref assoc (list (-> data second :coll) (-> data first)) data))))

(defn combined-search' 
  "derives from lambdaroyal.memory.abstraction.search/combined-search - 
  higher order function - takes a aggregator function [agr], a sequence of concrete functions (abstract-search), a query parameter [query] (can be a sequence) and optional parameters [opts] and returns go block. all the search functions [fns] are called with parameter [query] executed in parallel and feed their result (a sequence of user-scope tuples from lambdaroyal memory) to the aggregator function [agr]. By default the resulting go routine waits for all fn in [fns] for delivering a result.

  The following options are accepted\n 
  :timeout value in ms after which the aggregator channel is closed, no more search results are considered.\n
  :minority-report number of search function fn in [fns] that need to result in order to close the aggregator channel is closed and no more search results are considered.\n
  :finish-callback a function with no params that gets called when the aggregator go block stops."
  [agr fns query & opts]
  (apply combined-search fns agr query opts))


(defn combined-search''
  "SET SEARCH - derives from lambdaroyal.memory.abstraction.search/combined-search - 
  higher order function - takes a sequence of concrete functions (abstract-search), a query parameter [query] (can be a sequence), a function [finish-callback]  and optional parameters [opts] and returns go block. all the search functions [fns] are called with parameter [query] executed in parallel and feed their result (a sequence of user-scope tuples from lambdaroyal memory) to the aggregator function sorted-set-aggregator. the finish-callback takes one paremeter, the dereferenced aggregator value, in layman terms, all the documents that were found by the aggregator. By default the resulting go routine waits for all fn in [fns] for delivering a result.

  The following options are accepted\\n 
  :timeout value in ms after which the aggregator channel is closed, no more search results are considered.\\n
  :minority-report number of search function fn in [fns] that need to result in order to close the aggregator channel is closed and no more search results are consfidered."
  [fns query finish-callback & opts] 
  (let [acc (ref #{})
        agr (partial set-aggregator acc)]
    (apply combined-search' agr fns query :finish-callback #(finish-callback @acc) opts)))


;; --------------------------------------------------------------------
;; BUILDING A DATA HIERARCHIE
;; CR - https://github.com/gixxi/lambdaroyal-memory/issues/1
;; This allows to build a hierarchie of documents as per hierarchie 
;; levels. A hierarchie level denotes an attribute of the document - 
;; either directly (first level attribute) or indirectly by applying 
;; a function to the document
;; --------------------------------------------------------------------

;;(t/ann hierarchie [t/Any t/Kw * -> t/Any])
(defn hierarchie 
  "[level] is variable arity set of keywords or function taking a document into account and providing back a category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs."
  [xs handler & levels]
  (if levels
    (let [level (first levels)
          next (rest levels)
          xs' (group-by #(level %) xs)
          xs'' (pmap 
                (fn [[k v]]
                  [[k (count v)]
                   (apply hierarchie v handler next)])
                xs')]
      xs'')
    ;;else
    (if handler (handler xs) 
        ;;else
        xs)))

(defn hierarchie-ext 
  "builds up a hierarchie where a node is given by it's key (level discriminator), a map containing extra info that are characteristic for (an arbitrary) document that fits into this hierarchie as well as all the matching documents classified by the values of the next category (if any) or the matching documents as subnodes.
  [level] is variable arity set of taking a document into account and providing back a tuple [category ext], where category is a keyword or function providing back the category of a document whereas ext is a keyword or function providing back the the characteristics of a document with respect to the category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs."
  [xs handler & levels]
  (if levels
    (let [level (first levels)
          next (rest levels)]
      (let [[level-category level-ext] level
            xs' (group-by #(level-category %) xs)
            xs'' (pmap 
                  (fn [[k v]]
                    [[k (count v) (if level-ext (level-ext (first v)) nil)]
                     (apply hierarchie-ext v handler next)])
                  xs')]
        xs''))
    ;;else
    (if handler (handler xs) 
        ;;else
        xs)))














