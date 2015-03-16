## Defining your datamodel

Imaging you got three entity types
* part-orders
* orders
* types

where each part-order instance must belong to order instance. 
In addition each part-order must reference a type instance as well well. 
Beside that the keys of the instances of all the entity types must be unique.

The subsequently picture points out those relations.

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/example.metamodell.png)

## Creating the context from the metamodel
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

(def ctx (create-context meta-model))
```

So just ordinary clojure sequence types. The relations between the entity types are modeled as referential integrity constraints here denoted
by *:foreign-key-constraints*

## Creating a transaction

Here a *transaction* is just a small wrapper around the context. All user-scope functions for inserting, selecting and updating documents require this transaction to be scoped.

```clojure
(def tx (create-tx ctx))
```

## Inserting a document into the database

Lets insert a type, a order and two part-orders into our database.

```clojure
(dosync
  (insert tx :type 1 {:name :gotcha})
  (insert tx :order 1 {:name "my-first-order"})
  (insert tx :part-order 1 {:type 1 :order 1 :what-do-i-want :speed})
  (insert tx :part-order 2 {:type 1 :order 1 :what-do-i-want :grace}))
```
## Selecting one element by key

Get back the most recently added part-order 

```clojure
(def x
  (dosync
    (select-first tx :part-order 2)))
```

## Updating a document

Lets update the second part-order to have some additional data as well as an updated attribute

```clojure
(dosync
  (alter-document tx :part-order x assoc :what-do-i-want :lipstick :how :superfast))
```

## Selecting documents

There a two ways to retrieve documents from the database; by *key* and by *indexed attribute sets*. Either way one has to
define a range over the the document keys or the value of an indexed attribute set by providing a start-condition and optionally a stop-condition. A condition is given by a comparative operator and an operand.

### Selecting by *key*

```clojure
(dosync
  (select tx :part-order >= 0 < 2))
```

Turns out to return the first part-order instance, since the second one with *key = 2* fails to match the *key* range 0 <= x < 2.

```clojure
(dosync
  (select tx :part-order >= 0))
```

This just returns all part-order instances.
## Deleting a document

Lets clean up a bit

```clojure
(dosync
  (delete tx :part-order 2))
```
