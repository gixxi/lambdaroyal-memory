This Document contains information on several performance unittests that scope the performance of main database functionality over the curse of time.



# Inserting data

Runtime for inserting 30000 complex documents with several secondary indexes and referential integrity constraints by 10 concurrent actors.

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/test/stats/30000insertsBy10Threads.png)

# Denormalizing data & building search hierarchies

* Runtime for inserting approx. 75000 documents into the database
* Denormalizing all 75000 documents
* Building a hierarchie of all 75000 denormalized documents denoting certain categories of interest

## What is denomalizing

In our example data is hierachical, meaning that a set of documents of collection :line-item references exactly one document of collection :part-order. In layman terms, one part-order contains several line-items; and one order contains several part-orders. The subsequently picture denotes this hierarchie.

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/performanceunittest.png)

# How to update the performance indicators herein

* run ```lein midje```
* execute all stats*.gnuplot scripts in *test/stats* directory

# Machine & runtime specs

* Memory:        3970036 kB
* CPU:           Intel(R) Core(TM) i5-2467M CPU @ 1.60GHz
* Java HotSpot(TM) 64-Bit Server VM (build 25.40-b25, mixed mode)
* org.clojure/clojure "1.7.0-alpha5"
