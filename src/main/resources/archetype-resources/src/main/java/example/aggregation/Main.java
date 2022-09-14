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

        SingleOutputStreamOperator<StringObjectMap> stream = inputStream
                .filter(new DroolsBasedFilterFunction(ruleEngineProvider, configuration))
                .keyBy(new DroolsBasedFilterFunction(ruleEngineProvider, configuration))
                .process(new CustomProcessor(ruleEngineProvider, configuration));

        // Sand to store
        IGenericStateStore genericStateStore = new ProxyBackedGenericStateStore(configuration);
        stream.addSink(new GenericTimeWindowAggregationStoreSink(genericStateStore));

        // Debug to output
        if (configuration.getMiscellaneousProperties().getBoolean("console-debug-sink-enabled")) {
            stream.addSink(new DebugSync<>());
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