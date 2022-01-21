#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.timer;

import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.MainTemplate;
import io.github.devlibx.flink.example.pojo.Order;
import org.apache.flink.api.java.utils.ParameterTool;
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

        DataStream<Order> orders = KafkaSourceHelper.flink1_12_2_KafkaSource(
                KafkaSourceHelper.KafkaSourceConfig.builder()
                        .brokers(parameter.get("brokers", "localhost:9092"))
                        .groupId(parameter.get("groupId", "1234"))
                        .topic(parameter.get("topic", "orders"))
                        .build(),
                env,
                "",
                "",
                Order.class
        );

        // Transformer
        DataStream<Alert> transformer = orders.keyBy(Order::getCustomerKey).process(new CustomProcessor());

        // Skin
        transformer.addSink(new PrintSinkFunction<>());
    }
}
