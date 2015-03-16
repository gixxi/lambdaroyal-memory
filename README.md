# lambdaroyal-memory
STM-based in-memory database storing persistent data structures

[Usage](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/blob/master/doc/usage.md)

## Classification

lambdaroyal-memory is an in-memory database, that primarily recides in the volatile main memory. Either occasionally or always write operations are flushed to a persistent backup storage.

## Conceptual Data Model

lambdaroyal-memory stores user data as key/value pairs, where the key is
an orderable literal and the value is an arbitrary document. More
precisily the document is at least immutable and optional
persistent. Here persistence relates to persistent data structure that
reflect all past modifications to the datastructure and are the
cornerstone of clojure's ability to handle state and identity as two
different things, which turns out to be fundamental for clojure's
multiversion concurrency control-backed implementation of software
transactional memory (STM).

key/value pairs are not unrelated. Each key/value pair belongs to a
named collection. So the user can group key/value pairs by a certain
cognitive model. But this cognitive model is not covered by
lambdaroyal-memory. For the opposite consider a relational database
management systems that relies a proper (meta) description of the tuples
stored in individual relations, more technical the user describes the
relations in terms of attributes, the value ranges, certain constraints
like primary key constraints and foreign key constraints, index
constraints and so on. Those manifest a necessary subset of the
cognitive or ontological model.
Here we stick to the key/value pairs grouped into named
collections. that is almost all lambdaroyal-memory knows on the meta
level - almost all. What's left are constraints on the keys and
attributes of the value of key/value pairs.

This constraint are fundamentally speaking functional constraints,
concrete instances are indexes to speed-up data retrival and to back
other constraint like unique-constraints.

### Data Model from an User Point of View

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/abstraction.high.png)

### Data Model from an Technical Point of View

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/abstraction.low.png)

## Meta Model

The meta model reflects the conceptual data model in technical terms
that brings all information to the stake the database management system
needs to perform necessary checks on the user operations like read,
insert and update; to speed up the same and more fundamental to process
a transaction (here ACI Transaction, A...atomar, C...consistent,
I...isolated)

The metamodel states the subsequently given facts on the elements of the
conceptual data model

* collection

where collection is a tuple of 

* name of the collection
* unique constraint
* further functional constraints
* indexes per collection
* eviction channel that serves as persistent backup
