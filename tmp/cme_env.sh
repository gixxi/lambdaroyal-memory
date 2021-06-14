#/bin/bash
#export mongodb_username="rocklogmongo"
#export mongodb_password="WGM8ioTWdhtO8CpV"
#export mongodb_posturl="test-frankfurt-shard-00-00.ijxnz.mongodb.net:27017,test-frankfurt-shard-00-01.ijxnz.mongodb.net:27017,test-frankfurt-shard-00-02.ijxnz.mongodb.net:27017/test?ssl=true&replicaSet=atlas-wijjq2-shard-0&authSource=admin&retryWrites=true&w=majority"
#export mongodb_preurl="mongodb://"

export JAVA_OPTS="-Dmongodb_username=rocklogmongo -Dmongodb_password=WGM8ioTWdhtO8CpV -Dmongodb_posturl=test-frankfurt-shard-00-00.ijxnz.mongodb.net:27017,test-frankfurt-shard-00-01.ijxnz.mongodb.net:27017,test-frankfurt-shard-00-02.ijxnz.mongodb.net:27017/test?ssl=true&replicaSet=atlas-wijjq2-shard-0&authSource=admin&retryWrites=true&w=majority -Dmongodb_preurl=mongodb://"
