(ns lambdaroyal.memory.migration.mongodb
  (:require [lambdaroyal.memory.eviction.core :as evict]
            [lambdaroyal.memory.core.context :refer :all]
            [lambdaroyal.memory.core.tx :refer :all]
            [lambdaroyal.memory.eviction.mongodb :as evict-mongodb]
            [lambdaroyal.memory.eviction.couchdb :as evict-couchdb]
            [lambdaroyal.memory.helper :refer :all]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.db :as md]
            [com.ashafa.clutch :as clutch]
            [cheshire.core :as json]
            [lambdaroyal.memory.helper :refer :all])
  (:import [lambdaroyal.memory.core ConstraintException]
           [lambdaroyal.memory.core.tx ReferrerIntegrityConstraint]
           [org.apache.log4j BasicConfigurator]
           [java.text SimpleDateFormat]))

;; --------------------------------------------------------------------
;; HELPER FUNCTIONS
;; --------------------------------------------------------------------


(reset! evict/verbose' true)

(reset! evict-mongodb/verbose true)
(reset! evict-couchdb/verbose true)

(defn start-coll [ctx coll]
  @(.start (-> @ctx coll :evictor) ctx [(get @ctx coll)]))

;; --------------------------------------------------------------------
;; 
;; --------------------------------------------------------------------

(defn migrate-to-mongodb [ctx tx db]
  (let [state (atom [])]
    (future (let [res  (dosync (reduce
                                (fn [acc coll]
                                  (conj acc [coll (doall (select tx coll))]))
                                          ;; empty accumulator - we add on constantly
                                []
                                          ;; we iterate over this - coll is one element
                                (keys @ctx)))

                            ;; I/O
                  _   (doseq [[coll records] res]
                        (doseq [partition (partition-all 1000 records)]
                          (let [json' (map #(assoc (last %) :_id (first %)) partition)
                                _ (println :insert :coll coll :batch (count partition))
                                _ (if (or (= coll :stock-order-item) (= coll "stock-order-item"))
                                    (doseq [stock-order-item json']
                                      (println :stock-order-item stock-order-item)
                                      (mc/insert db coll stock-order-item)))
                                ;; _ (doseq [rec json']
                                ;;     (let [_ (println coll rec)
                                ;;           _ (mc/insert db coll rec)]))
                                ;; _ (mc/insert-batch db coll json')
                                ]))
                        (swap! state conj coll))]))
    state))

;; (defn migrate-to-mongodb [ctx tx db]
;;   (let [state (atom [])]
;;     (let [res  (dosync (reduce
;;                         (fn [acc coll]
;;                           (conj acc [coll (doall (select tx coll))]))
;;                                           ;; empty accumulator - we add on constantly
;;                         []
;;                                           ;; we iterate over this - coll is one element
;;                         (keys @ctx)))

;;                             ;; I/O
;;           _   (doseq [[coll records] res]
;;                 (doseq [partition (partition-all 1000 records)]
;;                   (let [json' (map #(assoc (last %) :_id (first %)) partition)
;;                         _ (println :insert :coll coll :batch (count partition))
;;                         _ (if (or (= coll :cardmeta) (= coll "cardmeta"))
;;                             (doseq [cardmeta json']
;;                               (println :cardmeta cardmeta)
;;                               (mc/insert db coll cardmeta)))]))
;;                 (swap! state conj coll))])
;;     state))