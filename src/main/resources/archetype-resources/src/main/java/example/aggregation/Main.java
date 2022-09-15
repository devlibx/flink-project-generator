#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;


import com.google.common.base.Objects;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.MainTemplateV2;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.SourceConfig;
import io.github.devlibx.miscellaneous.flink.drools.DebugSync;
import io.github.devlibx.miscellaneous.flink.drools.DroolsBasedFilterFunction;
import io.github.devlibx.miscellaneous.flink.drools.IRuleEngineProvider;
import io.github.devlibx.miscellaneous.flink.store.GenericTimeWindowAggregationStoreSink;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.ProxyBackedGenericStateStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main implements MainTemplateV2.RunJob<Configuration> {

    void internalRun(StreamExecutionEnvironment env, Configuration configuration, DataStream<StringObjectMap> inputStream, Class<Configuration> aClass) {

        // Make sure we have a good configuration
        configuration.validate();

        // Create rule engine
        String ruleFileLink = configuration.getRuleEngine().getRuleByName("main")
                .orElseThrow(() -> new RuntimeException("Did not find rule with name=main config file"))
                .getUrl();
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(ruleFileLink);

        // Step 1 - Filter input stream
        SingleOutputStreamOperator<StringObjectMap> filteredStream = inputStream
                .filter(new DroolsBasedFilterFunction(ruleEngineProvider, configuration))
                .name("filter").uid("d16903ba-3524-11ed-a261-0242ac120002");

        // Step 2 - Key by some id e.g. user id (mandatory for state-processor to have keyed stream)
        KeyedStream<StringObjectMap, String> keyedStream = filteredStream.keyBy(new DroolsBasedFilterFunction(ruleEngineProvider, configuration));

        // Step 3 - Do the custom processing
        SingleOutputStreamOperator<StringObjectMap> processedStream = keyedStream.process(new CustomProcessor(ruleEngineProvider, configuration))
                .name("processed-stream").uid("d16904b4-3524-11ed-a261-0242ac120002");

        // Step 4.1 - Send it to store
        IGenericStateStore genericStateStore = new ProxyBackedGenericStateStore(configuration);
        processedStream.addSink(new GenericTimeWindowAggregationStoreSink(genericStateStore))
                .name("store-sink").uid("d169059a-3524-11ed-a261-0242ac120002");

        if (configuration.getMiscellaneousProperties().getBoolean("console-debug-sink-enabled")) {
            // Step 4.2 - Send it to debug
            processedStream.addSink(new DebugSync<>())
                    .name("debug-sink").uid("d169066c-3524-11ed-a261-0242ac120002");
        }
    }

    @Override
    public void run(StreamExecutionEnvironment env, Configuration configuration, Class<Configuration> aClass) {
        // Filter and process
        SourceConfig sourceConfig = configuration.getSourceByName("mainInput")
                .orElseThrow(() -> new RuntimeException("Did not find source with name=mainInput in config file"));

        internalRun(env, configuration, sourceConfig.getKafkaSourceWithStringObjectMap(env), aClass);
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
        MainTemplateV2 template = new MainTemplateV2();
        template.main(args, jobName, job, Configuration.class);
    }
}
