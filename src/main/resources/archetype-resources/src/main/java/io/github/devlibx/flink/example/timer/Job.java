#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.timer;

import io.github.devlibx.flink.pojo.EventDeserializationSchema;
import io.github.devlibx.flink.pojo.Order;
import io.github.devlibx.flink.utils.Main;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

public class Job implements Main.RunJob {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        Main.main(args, job);
    }

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) {

        // Setup kafka source
        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers(parameter.get("brokers", "localhost:9092"))
                .setTopics(parameter.get("topic", "orders"))
                .setGroupId(parameter.get("groupId", "1234"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                .build();
        DataStream<Order> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transformer
        DataStream<Alert> transformer = kafkaStream.keyBy(Order::getCustomerKey).process(new CustomProcessor());

        // Skin
        transformer.addSink(new PrintSinkFunction<>());
    }
}
