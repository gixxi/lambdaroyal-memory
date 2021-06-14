## Using MongoDB as persistence backend within your tests

You need to do the following in the REPL in order to perform

> test_mongodb.clj

* set system variables for *mongodb_username*, *mongodb_password*, *mongodb_posturl*, *mongodb_preurl*, *mongodb_dbname*

either by REPL

```
(System/setProperty "mongodb_username" "foo") 
(System/setProperty "mongodb_password" "bar")
(System/setProperty "mongodb_posturl" "server.mongodb.net:27017/test?ssl=true&replicaSet=atlas-wijjq2-shard-0&authSource=admin&retryWrites=true&w=majority")  
(System/setProperty "mongodb_preurl" "mongodb://") 
(System/setProperty "mongodb_dbname" "test")
```

