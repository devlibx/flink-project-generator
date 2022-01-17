#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.MainTemplate;
import ${package}.pojo.FlattenLogEvent;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.Serializable;
import java.util.UUID;

public class Job implements MainTemplate.RunJob {

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setAutoWatermarkInterval(3000);

        // Create a kafka source
        DataStream<LogEvent> orders = KafkaSourceHelper.flink1_14_2_KafkaSource(
                KafkaSourceHelper.KafkaSourceConfig.builder()
                        .brokers(parameter.getRequired("LogEventJob.input.brokers"))
                        .groupId(parameter.getRequired("LogEventJob.input.groupId"))
                        .topic(parameter.getRequired("LogEventJob.input.topic"))
                        .build(),
                env,
                "LogKafkaStream",
                UUID.randomUUID().toString(),
                LogEvent.class
        );


        // Process data
        SingleOutputStreamOperator<FlattenLogEvent> outputStream = new Pipeline().process(
                env,
                orders,
                StringObjectMap.of(
                        "windowDuration", parameter.getInt("EventCountJob.windowDuration", 60),
                        "emitResultEventNthSeconds", parameter.getInt("EventCountJob.emitResultEventNthSeconds", 60)
                )
        );

        // Setup kafka sink as output
        KafkaSink<FlattenLogEvent> kafkaSink = KafkaSourceHelper.flink1_14_2_KafkaSink(
                KafkaSourceHelper.KafkaSinkConfig.builder()
                        .brokers(parameter.getRequired("LogEventJob.output.brokers"))
                        .topic(parameter.getRequired("LogEventJob.output.topic"))
                        .build(),
                new ObjectToKeyConvertor(),
                FlattenLogEvent.class
        );
        outputStream.sinkTo(kafkaSink).name("KafkaSink").uid(UUID.randomUUID().toString());
        // outputStream.addSink(kafkaSink).name("KafkaSink").uid(UUID.randomUUID().toString());
    }

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        MainTemplate.main(args, "LogEventJob", job);
    }

    private static class ObjectToKeyConvertor implements KafkaSourceHelper.ObjectToKeyConvertor<FlattenLogEvent>, Serializable {
        @Override
        public byte[] key(FlattenLogEvent fe) {
            return (fe.getIdType() + "-" + fe.getId()).getBytes();
        }
    }
}

