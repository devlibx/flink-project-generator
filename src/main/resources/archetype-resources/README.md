This example reads from a kafka topic and adds data to map state. When a timer of 1 sec 
expires, takes an action:

#####
Generate sample kafka data
```shell
./kafka-tpch load --brokers localhost:9092
```

#### Help
Copy config file to S3
```shell
aws s3 cp config.properties s3://<your bucket>/flink/   
```

#### Run
```shell
# change the jar name and path
java -cp target/my-app-1.0-SNAPSHOT.jar:.  --bucket <bucket> --file <file> com.myapp.Job 
```

### Sample commands
```shell
mvn archetype:create-from-project
```


#### Setup Flink
1. Download and extract flink "version flink-1.12.2"
2. Copy following jars in "lib" dir
```shell
<install dir>/opt/flink-s3-fs-hadoop-1.12.2.jar <install dir>/lib/
<install dir>/opt/flink-s3-fs-presto-1.12.2.jar <install dir>/lib/
```
3. Start cluster
```shell
# From bin dir
./start-cluster.sh

Launch:
http://localhost:8081/
```