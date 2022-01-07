#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.timer;

import io.github.devlibx.easy.flink.utils.ConfigReader;
import io.github.devlibx.easy.flink.utils.JsonMessageToEventDeserializationSchema;
import io.github.devlibx.easy.flink.utils.MainTemplate;
import ${package}.pojo.Order;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.walkthrough.common.entity.Alert;

public class Job implements MainTemplate.RunJob {
    public static void main(String[] args) throws Exception {
        Job job = new Job();
        MainTemplate.main(args, "ExampleJob", job);
    }

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) {

        // Setup kafka source
        KafkaSource<Order> source = KafkaSource.<Order>builder()
                .setBootstrapServers(parameter.get("brokers", "localhost:9092"))
                .setTopics(parameter.get("topic", "orders"))
                .setGroupId(parameter.get("groupId", "1234"))
                .setStartingOffsets(ConfigReader.getOffsetsInitializer(parameter))
                .setValueOnlyDeserializer(new JsonMessageToEventDeserializationSchema<>(Order.class))
                .build();

        DataStream<Order> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transformer
        DataStream<Alert> transformer = kafkaStream.keyBy(Order::getCustomerKey).process(new CustomProcessor());

        // Skin
        transformer.addSink(new PrintSinkFunction<>());
    }
}
