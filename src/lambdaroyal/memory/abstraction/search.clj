(ns lambdaroyal.memory.abstraction.search
  (:require [clojure.core.async :refer [>! <! alts! timeout chan go]]
            [lambdaroyal.memory.core.tx :as tx])
  (import [lambdaroyal.memory.core.tx Index ReferrerIntegrityConstraint]))

;; --------------------------------------------------------------------
;; TYPE ABSTRACTIONS
;; --------------------------------------------------------------------

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

(defn hierarchie
  "[level] is variable arity set of keywords or function taking a document into account and providing back a category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs."
  [xs handler & levels]
  (if levels
    (let [level (first levels)
          next (rest levels)
          xs' (group-by #(level %) xs)
          xs'' (pmap 
                (fn [[k v]]
                  ;;consider partial hierarchies, where a level is not present
                  (if k 
                    [[k (count v)]
                     (apply hierarchie v handler next)]
                    ;;else
                    (apply hierarchie v handler next)))
                xs')
          ;;special handling for partial hierarchies, if their is just one result in the bucket we
          ;;use this rather than the bucket 
          xs'' (if (and (= (count xs'') 1) next)
                 (first xs'') xs'') ]
      xs'')
    ;;else
    (if handler (handler xs) 
        ;;else
        xs)))

(defn hierarchie-backtracking-int
  [initial xs handler λbacktracking & levels]
  (let [res (if levels
              (let [level (first levels)
                    next (rest levels)
                    xs' (group-by #(level %) xs)
                    xs'' (map 
                          (fn [[k v]]
                            ;;atomic denotes whether a seq with just one element was present -> only the single element is returned
                            (let [res-from-recursion (apply hierarchie-backtracking-int false v handler λbacktracking next)]
                              ;;consider partial hierarchies, where a level is not present
                              (if k
                                (let [;;denotes whether the data came from leaf
                                      leaf (if (and (meta res-from-recursion) (-> res-from-recursion meta :leaf true?) (instance? clojure.lang.IPending res-from-recursion))
                                             true false)
                                      res-from-recursion (if leaf
                                                           @res-from-recursion
                                                           res-from-recursion)]
                                  [(λbacktracking leaf [k (count v)] res-from-recursion) res-from-recursion])
                                ;;else
                                res-from-recursion)))
                          xs')
                    ;;special handling for partial hierarchies, if their is just one result in the bucket we
                    ;;use this rather than the bucket 
                    xs'' (if (= (count xs'') 1) (first xs'') xs'')]
                xs'')
              ;;else
              ;;what the hack, we need to know whether we got data from the final recursion
              ;;furthermore this data might be provided back without using grouping it due to
              ;;partial hierarchies, types would be nice
              (deliver (with-meta (promise) {:leaf true})
                       (if handler (handler xs) 
                           ;;else
                           xs)))]
    (if (and initial (meta res) (-> res meta :leaf) (instance? clojure.lang.IPending res))
      @res
      res)))

(defn hierarchie-backtracking
  "[level] is variable arity set of keywords or function taking a document into account and providing back a category. [handler] is a function applied to the leafs of the hierarchie. Using identity as function will result the documents as leafs. 
  λbacktracking-fn must accept boolean flag that denotes whether we inspect leafs, the group key k, and a sequence of elements that result from applying a level discriminator to xs. k is [level-val count], where level-val denotes the result of applying the level discrimator function, count the number of elements WITHIN the next recursion matching the category. The function must return a adapted version of k that reflects the information necessary to the user."
  [xs handler λbacktracking & levels]
  (apply hierarchie-backtracking-int true xs handler λbacktracking levels))

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
                    ;;consider partial hierarchies, where a level is not present
                    (if k
                      [[k (count v) (if level-ext (level-ext (first v)) nil)]
                       (apply hierarchie-ext v handler next)]
                      (apply hierarchie-ext v handler next)))
                  xs')]
        xs''))
    ;;else
    (if handler (handler xs) 
        ;;else
        xs)))

(defn ric
  "returns the first identified ReferrerIntegrityConstraint from a collection [source] that refers to a collection [target]. If the [foreign-key] is given then we explicitly seach for a ric with a certain foreign-key."
  ([tx source target]
   {:pre [(contains? (-> tx :context deref) source)
          (contains? (-> tx :context deref) target)]}
   (let [ctx (-> tx :context deref)
         source-coll (get ctx source)
         target-coll (get ctx target)
         constraints (map last (-> source-coll :constraints deref))
         rics (filter
               #(instance? ReferrerIntegrityConstraint %) constraints)] 
     (some #(if (= (.foreign-coll %) target) %) rics)))
([tx source target foreign-key]
   {:pre [(contains? (-> tx :context deref) source)
          (contains? (-> tx :context deref) target)]}
   (let [ctx (-> tx :context deref)
         source-coll (get ctx source)
         target-coll (get ctx target)
         constraints (map last (-> source-coll :constraints deref))
         rics (filter
               #(instance? ReferrerIntegrityConstraint %) constraints)] 
     (some #(if (and 
                 (= (.foreign-coll %) target)
                 (= (.foreign-key %) foreign-key)) %) rics))))

(defn by-ric
  "returns all user scope tuples from collection [source] that refer to collection [target] by some ReferrerIntegrityConstraint and have foreign-key of the sequence [keys]. the sequence is supposed to be redundancy free (set). opts might contain :ratio-full-scan iff greater or equal to the ratio (count keys / number of tuples in target of [0..1]) then the source collection is fully scanned for matching tuples rather than queried by index lookups. If not given, 0.4 is the default barrier. If the :foreign-key is given within the opts then we explicitly seach for a ric with a certain foreign-key."
  ([tx source target keys & opts]
   (with-meta 
     (let [opts (if opts (apply hash-map opts))
           {foreign-key :foreign-key verbose :verbose parallel :parallel, ratio-full-scan :ratio-full-scan, :or {parallel true ratio-full-scan 0.4 verbose false}} opts
           ric (or (if foreign-key 
                     (ric tx source target foreign-key) (ric tx source target)) 
                   (throw (IllegalStateException. (str "Failed to build data projection - no ReferrerIntegrityConstraint from collection " source " to collection " target " defined."))))
           target-count (-> tx :context deref target :data deref count)
           keys-to-collsize-ratio (if (= target-count 0) 0 (/ (count keys) target-count))
           full-scan (> keys-to-collsize-ratio ratio-full-scan)
           _ (if verbose (println :search-by-ric-keys :ric source (.foreign-key ric) "->" target :keys (count keys) :target-count target-count :ratio keys-to-collsize-ratio))
           _ (if (and verbose full-scan) (println :full-scan source target keys-to-collsize-ratio))
           ctx (-> tx :context deref)
           source-coll (get ctx source)]
       (if (= target-count 0)
         []
         ;;else
         (if full-scan
           ;;in full-scan mode we just build a set of the keys provided and filter
           (let [keys (into #{} keys)
                 xs (tx/select tx source)]
             (filter #(contains? keys (get (last %) (.foreign-key ric))) xs))
           (let [find-fn (fn [key]
                           (take-while  
                            #(= key (get (last %) (.foreign-key ric)))
                            (map tx/user-scope-tuple
                                 (tx/select-from-coll 
                                  source-coll 
                                  [(.foreign-key ric)]
                                  >= 
                                  [key]))))
                 ;;one seq with the results for each key
                 xs ((if parallel pmap map) find-fn keys)]
             (reduce 
              (fn [acc x]
                (concat acc x)) [] xs)))))
     {:coll-name target})))

(defn >>
  "Pipe, Convenience Function. A higher order function that returns a function taking a transaction [tx] and a set of user scope tupels into account that MUST aggregate as meta data key :coll-name the collection the tupels belong to, the lambda returns itself a by-ric from source collection as per the input xs to target location. The filter is applied to the xs. If the :foreign-key is given within the opts then we explicitly seach for a ric with a certain foreign-key."
  [target filter-fn & opts]
  (fn [tx xs]
    {:pre [(-> xs meta :coll-name)]}
    (with-meta (filter filter-fn
                       (apply by-ric tx target (-> xs meta :coll-name) (map first xs) opts))
      {:coll-name target})))

(defn >>>
  "Pipe, Convenience Function. A higher order function that returns a function taking a transaction [tx] and a set of user scope tupels into account that MUST aggregate as meta data key :coll-name the collection the tupels belong to, the lambda returns itself a by-ric from source collection as per the input xs to target location. If the :foreign-key is given within the opts then we explicitly seach for a ric with a certain foreign-key."
  [target & opts]
  (apply >> target (fn [x] true) opts))

(defn filter-all
  "higher order function that returns a function that returns a sequence of all tuples within the collection with name [coll-name]"
  [tx coll-name]
  (let [meta {:coll-name coll-name}]
    (with-meta (fn []
                 (with-meta 
                   (tx/select tx coll-name) meta)) meta)))

(defn filter-key
  "higher order function that returns a function that returns a sequence of all tupels whose key is equal to [key]"
  ([tx coll-name key]
   (let [meta {:coll-name coll-name}]
     (with-meta (fn [& opts]
                  (with-meta
                    (take-while  
                     #(= key (first %))
                     (tx/select tx coll-name >= key))
                    meta)) meta)))
  ([tx coll-name start-test start-key]
   (let [meta {:coll-name coll-name}]
     (with-meta (fn [& opts]
                  (with-meta (tx/select tx coll-name start-test start-key) meta)) meta)))
  ([tx coll-name start-test start-key stop-test stop-key]
   (let [meta {:coll-name coll-name}]
     (with-meta (fn [& opts]
                  (with-meta (tx/select tx coll-name start-test start-key stop-test stop-key))
                  meta) meta))))

(defn filter-index
  "higher order function that returns a function that returns a sequence of all tupels that are resolved using a index lookup using the attribute seq [attr] and the comparator [start-test] as well as the attribute value sequence [start-key]. The second version 'lambdas' the index search for a range additionally taking [stop-test] and [stop-key] into account"
  ([tx coll-name attr start-test start-key]
   (let [meta {:coll-name coll-name}]
     (with-meta (fn [& opts]
                  (with-meta (tx/select tx coll-name attr start-test start-key)
                    meta))
       meta)))
  ([tx coll-name attr start-test start-key stop-test stop-key]
   (let [meta {:coll-name coll-name}]
     (with-meta 
       (fn [& opts]
         (with-meta (tx/select tx coll-name attr start-test start-key stop-test stop-key) meta))
       meta))))

(defn proj 
  "data projection - takes a higher order functions λ into account that that returns a function whose application results in a seq of user scope tupels AND metadata with :coll-name denoting the collection the tupels belong to. Furthermore this function takes a variable number of path functions [path-fns] into account. The first one is supposed to take the outcome of application of λ into account, all others are supposes to take the outcome of the respective predessor path-fn into account. All are supposed to produce a seq of user scope tupels into account that is consumable be the respective successor path-fn AND metadata denoting the collection name by key :coll-name."
  [tx λ & path-fns]
  ;;this fuss means we copy the meta data from the function to the result of its application
  (let [xs (with-meta (λ) (meta λ))]
    (if-not path-fns 
      xs
      (loop [xs xs path-fns path-fns]
        (if 
            (empty? path-fns) xs
            (recur ((first path-fns) tx xs) (rest path-fns)))))))
