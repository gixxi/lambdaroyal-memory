# Datastructures

## User-scope values and coll-tuples

When a user inserts a value (user-scope value) into the database this value gets wrapped into a STM ref. Further function applications are applied indirectly to the STM ref via a user-scope database function alter-document. The user has no direct access to the STM since direct function application would bypass index altering.

## Indexes and Constraints

An index are is a special kind of constraint. The constraint contract is fullfilled in order to check BEFORE an insert or an update whether the data fullfiles the constraint (e.g. being unique with respect to a certain attribute)

Furthermore the index serves as speed-up for selecting data based on data attributes rather than the key of the key/value pair the data is incooperating in.

### Indexes are functional

Indexes do not directly refer to atomic attributes of data but to a a comparator function that takes two data items into account and can act on arbitrary attributes or relations between them of that very data.

### How is a Index used within a transaction

####Invariant

No matter whether what the constraint contract part of the index is executed whenever a user alters a STM ref of a value that is supported by indexes.

Technically indexes are associated to a collection like all other kinds of constraints and adapted whenever the user changes a value using the according database functions.

### Referential Integrity Constraints

A Referential Integrity Constraint (RIC) is defined by a domain dependency from a document A to a document B. More precisely an RIC is a function fn [a fk C] where a is a document, fk denotes the foreign key attribute by name within a whereas C is the collection where a document must exist with key = value of fk in document a. The function throws an ConstraintException when the RIC is not fullfilled. The RIC is checked when a document gets inserted into a referencing collection or deleted from a referenced collection. 

A RIC is backed by an index (subject to change: index must not be deleted when depending RIC is present) to allow efficent constraint checks during delete.