#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.window.eventcount;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.functions.common.EventCount;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.MainTemplate;
import io.github.devlibx.flink.example.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.UUID;

@Slf4j
public class EventCountJob implements MainTemplate.RunJob {

    public static void main(String[] args) throws Exception {
        EventCountJob job = new EventCountJob();
        MainTemplate.main(args, "EventCountJob", job);
    }

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(3000);

        // Create a kafka source
        DataStream<Order> orders = KafkaSourceHelper.flink1_14_2_KafkaSource(
                KafkaSourceHelper.KafkaSourceConfig.builder()
                        .brokers(parameter.get("EventCountJob.input.brokers", "localhost:9092"))
                        .groupId(parameter.get("EventCountJob.input.groupId", "1234"))
                        .topic(parameter.get("EventCountJob.input.topic", "orders"))
                        .build(),
                env,
                "OrdersKafkaStream",
                UUID.randomUUID().toString(),
                Order.class
        );

        // Process data
        new Pipeline().process(
                env,
                orders,
                StringObjectMap.of(
                        "windowDuration", parameter.getInt("EventCountJob.windowDuration", 60),
                        "emitResultEventNthSeconds", parameter.getInt("EventCountJob.emitResultEventNthSeconds", 60)
                )
        ).addSink(sink()).name("PrintSink").uid(UUID.randomUUID().toString());
        // The output of "Pipeline" class is sent to a Print sink -> you can send to some topic if required

        // Multi-Source example - One more sink from same source
        // orders.addSink(new PrintSinkFunction<>()).name("PrintSink2").uid(UUID.randomUUID().toString());
    }

    protected SinkFunction<EventCount> sink() {
        return new PrintSinkFunction<>();
    }
}
