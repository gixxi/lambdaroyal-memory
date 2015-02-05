# lambdaroyal-memory
STM-based in-memory database storing persistent data structures

## Classification

lambdaroyal-memory is a in-memory database, that primarily recides in the volatile main memory. Either occasionally or always write operations are flushed to a persistent backup storage.

## Conceptual Data Model

lambdaroyal-memory stores user data as key/value pairs, where the key is an orderable literal and the value is an arbitrary document. More precisily the document is at least immutable and optional persistent. Here persistence relates to persistent data structure that reflect all past modifications to the datastructure and are the cornerstone of clojure's ability to handle state and identity as two different things, which turns out to be fundamental for clojure's multiversion concurrency control-backed implementation of software transactional memory (STM).