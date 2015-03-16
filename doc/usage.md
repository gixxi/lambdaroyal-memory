# Defining your datamodel

Imaging you got three entity types
* part-orders
* orders
* types

where each part-order instance must belong to order instance. 
In addition each part-order must reference a type instance as well well. 
Beside that the keys of the instances of all the entity types must be unique.

The subsequently picture points out those relations.

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/example.metamodell.png)

# Creating the context from the metamodel
```clojure
(ns lambdaroyal.memory.core.test-context
  (require [lambdaroyal.memory.core.context :refer :all]))

(def meta-model-with-ric
  {:type
   {:unique true :indexes []}
   :order
   {:unique true :indexes []}
   :part-order
   {:unique true :indexes [] :foreign-key-constraints [
                                                       {:name :type :foreign-coll :type :foreign-key :type}
                                                       {:name :order :foreign-coll :order :foreign-key :order}]}})
```

So just ordinary clojure sequence types. The relations between the entity types are modeled as referential integrity constraints here denoted
by *:foreign-key-constraints*
