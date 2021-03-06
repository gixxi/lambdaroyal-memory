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

Denormalizing gives back a flat structure denoted by the following picture. From a technical point of view denormalizing is performed by replacing a foreign key by the document denoted by the foreign key. In layman terms, denormalizing performs all necessary read operations that get all the foreign data (directly and transitively) referenced by the document passed into the denormalization function.

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/design/current.datastructures/performanceunittest_denormalized.png)

## What is hierarchie

Here hierachie denotes grouping the data into certain categories. For each value of the first category the grouping is applied recursivly to the rest of the categories. One can use such a hierarchie to search for documents matching certain category values.

## Performance

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/test/stats/1000Orders.png)

# Projecting Data

Joining from collection A to B to C, where we consider only a certain a of A, C contains aprox. 1500 tupels. 3 Threads do this concurrently.

## What is projecting data

Projecting data is like querying relations using joins in relational database, using projections one can search for certain data that is associated using referential integrity constraints.

## Performance

![](https://raw.githubusercontent.com/gixxi/lambdaroyal-memory/master/test/stats/projection.png)


# How to update the performance indicators herein

* run ```lein midje```
* execute all stats*.gnuplot scripts in *test/stats* directory OR run *make* in *test/stats*

# Machine & runtime specs

* Memory:        4 GB RAM, 1300 Mhz
* CPU:           Intel(R) Core(TM) i5-2467M CPU @ 1.60GHz, 2 Cores
* Java HotSpot(TM) 64-Bit Server VM (build 25.40-b25, mixed mode)
* org.clojure/clojure "1.7.0-alpha5"

Starting from *2016-08-17* we perform this tests on a 

* Memory:        16 GB RAM, 2100 Mhz
* CPU:           Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz, 4 Cores + Hyperthreading
* Java(TM) SE Runtime Environment (build 1.8.0_91-b14)
* org.clojure/clojure "1.8.0-RC4"
