;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;   _________ .__             __                                       
;;   \_   ___ \|  |   ____    |__|__ _________   ____                   
;;   /    \  \/|  |  /  _ \   |  |  |  \_  __ \_/ __ \                  
;;   \     \___|  |_(  <_> )  |  |  |  /|  | \/\  ___/                  
;;    \______  /____/\____/\__|  |____/ |__|    \___  >                 
;;           \/           \______|                  \/                  
;;    ____ ___                       ________                           
;;   |    |   \______ ___________   /  _____/______  ____  __ ________  
;;   |    |   /  ___// __ \_  __ \ /   \  __\_  __ \/  _ \|  |  \____ \ 
;;   |    |  /\___ \\  ___/|  | \/ \    \_\  \  | \(  <_> )  |  /  |_> >
;;   |______//____  >\___  >__|     \______  /__|   \____/|____/|   __/ 
;;                \/     \/                \/                   |__|    
;;
;;   STM-based Database lambdaroyal-memory
;;   https://github.com/gixxi/lambdaroyal-memory
;;   * In-memory store for clojure data structures with optional persistence layer
;;   * hassle-free indexes, constraints, search, analytics on your precious data
;;   * Datomic, SAP Hana for the poor people
;;   * FreeBSD License
;;   * backed by clojure.core, core.async, clojure.spec, (CouchDB, ...)
;;
;;   christian.meichsner@live.com - http://www.planet-rocklog.com
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(do
  (require 
   '[lambdaroyal.memory.ui-helper :refer :all]
   '[lambdaroyal.memory.core.tx :refer :all]
   '[lambdaroyal.memory.core.context :refer :all]
   '[lambdaroyal.memory.abstraction.search :refer :all]
   '[lambdaroyal.memory.helper :refer :all])
  (import [lambdaroyal.memory.core ConstraintException]
          [org.apache.log4j BasicConfigurator]))

(BasicConfigurator/configure)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; slurp-in some demodata
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def articles (slurp-csv "doc/resources/articles.csv" 
                         :ident string-to-string	
                         :client string-to-string	
                         :unit string-to-string
                         :price string-to-float 
                         :inboundAmount string-to-long))

(def stocks (slurp-csv "doc/resources/stocks.csv"
                       :article string-to-long	
                       :amount string-to-float
                       :batch string-to-string	
                       :color string-to-string
                       :size string-to-string))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DEFINE YOUR METAMODEL - statically/extendable at runtime
;; each collection is given by
;; name, indexes (atomic attributes or tupels of attributes), RICs (referential
;; integrity contraints, ...)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def model
  {:article
   {:indexes [{:attributes [:ident]}
              {:attributes [:client]}]}
   :stock
   {:indexes [{:attributes [:batch]}] :foreign-key-constraints [{:foreign-coll :article :foreign-key :article}]}})




;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GET A (RUNTIME) CONTEXT FROM YOUR METAMODEL
;;
;; ns lambdaroyal.memory.core.context
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ctx (create-context model))





;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; THE CONTEXT SCOPES DATA, CONTRAINTS AND OPTIONALLY AN EVICTION CHANNEL (I/O)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(-> ctx deref :stock keys)



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CREATE SOME TRANSACTION CONTEXT (TX) -> ALL THE CRUD FUNCTIONS TAKE THE TX
;; AS FIRST ARGUMENT
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def tx (create-tx ctx))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; READ-IN OUR SLURPS -> will fail when not in compliance with model
;; all data we insert is given as keyval, where key is some atomic value and
;; value is a map. We cal the keyval UST (User Space Tupel)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(dosync
 (doseq [x stocks]
   (insert tx :stock (first x) (last x))))

(dosync
 (doseq [x articles]
   (insert tx :article (first x) (last x))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CRUD SELECT - select by id, range, index or just using the lazy seq
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;



(select-first tx :article 1)
(select tx :article < 10)
(select tx :article > 10 <= 12)
(time (first (select tx :article [:ident] >= ["Kiwi"])))
(take 3 (select tx :article))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CRUD INSERT - Inserting a document into the database
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(dosync (insert tx :stock (rand-int (Integer/MAX_VALUE)) {:article -1 :batch "what the f... is -1"}))


(def kiwi' (first (select tx :article [:ident] >= ["Kiwi"])))

(def stock' (dosync (insert tx :stock (rand-int (Integer/MAX_VALUE)) {:article (first kiwi') :batch "my first and only kiwi" :amount 1})))

(def stock' (first (select tx :stock [:batch] >= ["my first and only kiwi"])))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CRUD UPDATE - Update a document 
;; using some λ (merge, update, assoc, dissoc, ...)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(dosync
 (alter-document tx :stock stock' assoc :article -1 :batch "RIC is ringing f...!"))

(dosync
 (alter-document tx :stock stock' assoc :batch2 "after duplication" :amount 2))

(dosync
 (alter-document tx :stock stock' merge {:color "green" :size "tiny"}))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; CRUD DELETE - Delete a document by it's id, returns 1 for the number of
;; deleted documents iff ok
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(dosync
 (delete tx :article (first kiwi')))

(dosync
 (delete tx :stock (first stock')))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SEARCH ABSTRACTIONS - Laziness and cores to the rescue
;; Span parallel search, combine results, handle timeouts (I bow to core.async)
;;
;; ns lambdaroyal.memory.abstraction.search
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;plain vanilla version - with set-aggregator, concat-aggregator is also available
;;an aggregator is a function taking a ref (e.g. atom, stm-ref, ...) as well as a 
;;result xs into account 
(defn search' [tx coll attr]
  (abstract-search 
   (fn [value]
     (filter
      (fn [x] 
        (.contains (or (get (last x) attr) "") value))
      (select tx coll)))))

(let [result (ref #{})
      start (System/currentTimeMillis)]
  (combined-search [(search' tx :stock :batch)
                    (search' tx :article :ident)]
                     (partial set-aggregator result) 
                     "ki" 
                     :finish-callback (fn [] (println "aggregation took (ms) " (- (System/currentTimeMillis) start) :result @result))))

;;minority report - take the result from just n search functions 
;;and just forget about everything else

(let [result (ref #{})
      start (System/currentTimeMillis)]
  (combined-search [(search' tx :stock :batch)                    
                    (search' tx :article :ident)
                    (search' tx :stock :color)
                    (search' tx :stock :size)]
                     (partial set-aggregator result) 
                     "Elektro" 
                     :minority-report 3
                     :finish-callback (fn [] (println "aggregation took (ms) " (- (System/currentTimeMillis) start) :result @result))))

;;timeout - take the result from search functions that return before timeout (ms)
;;and just forget about everything else

(let [result (ref #{})
      start (System/currentTimeMillis)]
  (combined-search [(search' tx :stock :batch)                    
                    (search' tx :article :ident)
                    (search' tx :stock :color)
                    (search' tx :stock :size)]
                     (partial set-aggregator result) 
                     "sm" 
                     :timeout 1
                     :finish-callback (fn [] (println "aggregation took (ms) " (- (System/currentTimeMillis) start) :result @result))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA PROJECTIONS - joining related data
;; e.g. having a set of articles we wanna get all the stocks that refer those
;; articles
;;
;; ns lambdaroyal.memory.abstraction.search 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;gettin' by id
(proj tx 
      (filter-key tx :article (first kiwi'))
      (>>> :stock))

;;gettin' just all
(count (proj tx 
             (filter-all tx :article)
             (>>> :stock :verbose true)))

(time (doall (proj tx 
                   (filter-key tx :article (first kiwi'))
                   (>>> :stock :verbose true))))

;;gettin' by index on article
(proj tx 
      (filter-index tx :article [:client] >= ["Pflanzen"] < ["Pflanzena"])
      (>>> :stock))

;;gettin' by custom λ
(proj tx 
      (with-meta
        (fn []
          (filter #(> (or (-> % last :price) 0) 100) (select tx :article)))
        {:coll-name :article})
      (>>> :stock))

;;rise up - gettin all with expensive article that are huge
(proj tx 
      (with-meta
        (fn []
          (filter #(> (or (-> % last :price) 0) 100) (select tx :article)))
        {:coll-name :article})
      (>> :stock #(= "huge" (-> % last :size))))

;;just imagin'
(comment (proj tx 
               (filter-key tx :article (first kiwi'))
               (>>> :stock)
               (>>> :stock-order-item)
               (>>> :transport)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; KEEPING YOUR DATA PERSISTENZ USING DATA EVICTORS
;;
;; ns lambdaroyal.memory.eviction.core
;; ns lambdaroyal.memory.eviction.couchdb
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(require 
 '[lambdaroyal.memory.eviction.core :as evict]
 '[lambdaroyal.memory.eviction.couchdb :as evict-couchdb])


;;just create an evictor and consider it when creating the model, you might have different 
;;evictors and evictor combinators
(def evictor (evict-couchdb/create :prefix "foo"))
(def model
  {:article
   {:indexes [{:attributes [:ident]}
              {:attributes [:client]}]
    :evictor evictor}
   :stock
   {:indexes [{:attributes [:batch]}] :foreign-key-constraints [{:foreign-coll :article :foreign-key :article}]
    :evictor evictor}})

;;starting the evictor checks for the couchdb connection and creates all the databases
(defn- start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))


(def ctx (create-context model))
(start-coll ctx :article)
(start-coll ctx :stock)

(def tx (create-tx ctx))

(dosync
 (doseq [x articles]
   (insert tx :article (first x) (last x))))

(dosync
 (doseq [x stocks]
   (insert tx :stock (first x) (last x))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Now we take everything done and start from scratch taking the data from the
;; couchdb 
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def evictor (evict-couchdb/create :prefix "foo"))
(def ctx (create-context model))
(start-coll ctx :article)
(start-coll ctx :stock)

(def tx (create-tx ctx))


(def x (select-first tx :article 1))

;; all writing operations are async reflected within the perstent layer
(dosync (alter-document tx :article x assoc :size "megahuge2"))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DATA HIERARCHIES - Building hierarchies groups data recursivly 
;; as per a set of discriminator functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(apply hierarchie-backtracking (select tx :stock) count
       (fn [k v b]
         v)
       [#(-> % last :size) #(-> % last :color)])



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ROADMAP
;; Prio 1 - using etcd and/or rocksdb as persistence backend
;; Prio 2 - WORM collection - storing certain data non-transactional, useful for large
;; sets like history data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
