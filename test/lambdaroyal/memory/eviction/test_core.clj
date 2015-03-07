(ns lambdaroyal.memory.eviction.test-core
  (require [midje.sweet :refer :all]
           [lambdaroyal.memory.eviction.core :refer :all]))

(facts "creating eviction scheme"
  (let [evictor (create-SysoutEvictionChannel)
        proxy (create-proxy evictor 3000)]))
