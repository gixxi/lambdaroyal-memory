This Document contains information on how to implement reverse indexes and range scans in lambdaroyal-memory.

reverse index lookup and reverse range scans traverse the data from the key with the highest rank to the lowests. rank here refers to the pos of a document key within a sorting index|index set.
So for monotonic (strictly goes here naturally since a key is unique) increasing numeric keys the newest document has the highest rank whereas the oldest document has the lowest rank.

# Functions that should be able to perform the reverse lookup

## range scan

```clojure
(require '[lambdaroyal.memory.core.tx :refer :all])
(select tx :stock-order :reverse)
```


# Additional benefits from this implementation

## index scans that checks for equality

```clojure
(require '[lambdaroyal.memory.core.tx :refer :all])
(select tx :stock-order [:ident] = ["gaga"])
``` 

## index scans that presume just one attribute in an index

```clojure
(require '[lambdaroyal.memory.core.tx :refer :all])
(select tx :stock-order :ident = "gaga")
(select tx :stock-order :ident >= "gaga")
``` 
