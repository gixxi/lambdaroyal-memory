(ns lambdaroyal.memory.core.test-multithreading-tx
  (:require [midje.sweet :refer :all]
           [lambdaroyal.memory.core.tx :refer :all]
           [lambdaroyal.memory.core.context :refer :all]
           [lambdaroyal.memory.helper :refer :all])
  (import [lambdaroyal.memory.core ConstraintException]))

(def ^:const colls 10)

(def meta-model
  (zipmap (range colls) (repeat {:unique true :indexes []})))

(defn- clerk 
  "this clerk chooses two account by random and transfers one buck from one to the other"
  [ctx]
  (dosync
   (let [tx (create-tx ctx)
         c1 (rand-int colls)
         c2 (rand-int colls)
         a1 (rand-int colls)
         a2 (rand-int colls)
         d1 (select-first tx c1 a1)
         d2 (select-first tx c2 a2)]
     (if (not= 0 (compare [c1 a1] [c2 a2]))
       (do
         (alter-document tx c1 d1 assoc :amount (-> d1 last :amount dec))
         (alter-document tx c2 d2 assoc :amount (-> d2 last :amount inc))))
     [c1 a1 c2 a2]
     )))


(defn- sum-accounts [tx c]
  (apply +
         (map #(-> % last :amount)
              (select tx c >= 0))))

(facts "check whether balance is kept when modifying withdraw/deposit from one account to another"
  (let [ctx (create-context meta-model)
        tx (create-tx ctx)]
    ;;lets insert all the accounts
    (dosync
     (doall
      (for [c (range colls) a (range colls)]
        (insert tx c a {:amount 0}))))
    ;;start all the clerks
    (fact "doing 2000 parallel account withdrawals/deposits must bot take more than 2 seconds" (first (timed (wait-futures 1000 (clerk ctx)))) => (roughly 0 1000))
    ;;check for the balance
    (fact "accounts must be in balance"
      (apply + (map #(sum-accounts tx %) (range colls))) => 0)))



















