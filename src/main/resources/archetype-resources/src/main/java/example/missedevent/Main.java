#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent;

import com.google.common.base.Objects;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.MainTemplate;
import ${package}.example.drools.DebugSync;
import ${package}.example.drools.DroolBasedKeyFinder;
import ${package}.example.drools.DroolsBasedFilterFunction;
import ${package}.example.drools.IRuleEngineProvider;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main implements MainTemplate.RunJob {

    @Override
    public void run(StreamExecutionEnvironment env, ParameterTool parameter) {

        // Create rule engine
        String ruleFileLink = parameter.getRequired("rule.file");
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(ruleFileLink);

        SingleOutputStreamOperator<StringObjectMap> stream = KafkaSourceHelper.flink1_14_2_KafkaSource(
                        KafkaSourceHelper.KafkaSourceConfig.builder()
                                .brokers(parameter.getRequired("input.brokers"))
                                .groupId(parameter.getRequired("input.groupId"))
                                .topic(parameter.getRequired("input.topic"))
                                .build(),
                        env,
                        "Job_Input_Stream",
                        "ed61f7d6-263b-11ed-a261-0242ac120002",
                        StringObjectMap.class
                )
                .filter(new DroolsBasedFilterFunction(ruleEngineProvider))
                .keyBy(new DroolsBasedFilterFunction(ruleEngineProvider))
                .process(new CustomProcessor(ruleEngineProvider, parameter.getInt("state.ttl", 24 * 60)));

        // Setup kafka sink as output
        KafkaSink<StringObjectMap> kafkaSink = KafkaSourceHelper.flink1_14_2_KafkaSink(
                KafkaSourceHelper.KafkaSinkConfig.builder()
                        .brokers(parameter.getRequired("output.brokers"))
                        .topic(parameter.getRequired("output.topic"))
                        .build(),
                new DroolBasedKeyFinder(ruleEngineProvider),
                StringObjectMap.class
        );
        stream.sinkTo(kafkaSink).name("KafkaSink").uid("ed61f7d6-263b-11ed-a261-0242ac120001");

        // Debug to output
        stream.addSink(new DebugSync<>());
    }

    public static void main(String[] args) throws Exception {
        String jobName = "MissingEventHandlerJob";
        for (int i = 0; i < args.length; i++) {
            if (Objects.equal(args[i], "--name")) {
                jobName = args[i + 1];
                break;
            }
        }

        Main job = new Main();
        MainTemplate.main(args, jobName, job);
    }
}
