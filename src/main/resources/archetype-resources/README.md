This example reads from a kafka topic and adds data to map state. When a timer of 1 sec 
expires, takes an action:

#####
Generate sample kafka data
```shell
./kafka-tpch load --brokers localhost:9092
```