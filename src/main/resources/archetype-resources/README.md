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