#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

public class Job {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("orders")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new EventDeserializationSchema())
                .build();
        DataStream<Order> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<Alert> input1 = input.keyBy(Order::getCustomerKey).process(new CustomProcessor());
        input1.addSink(new PrintSinkFunction<>());
        env.execute("Fraud Detection");
    }
}
