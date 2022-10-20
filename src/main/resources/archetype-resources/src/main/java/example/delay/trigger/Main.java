#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.delay.trigger;


import com.google.common.base.Objects;
import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.flink.utils.v2.MainTemplateV2;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.flink.utils.v2.config.KafkaSinkConfig;
import io.github.devlibx.easy.flink.utils.v2.config.SourceConfig;
import io.github.devlibx.miscellaneous.flink.drools.DebugSync;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.Serializable;
import java.util.UUID;

@SuppressWarnings("deprecation")
public class Main implements MainTemplateV2.RunJob<Configuration> {
    public static final String ID_PARAM_NAME = "idempotency_id";

    void internalRun(StreamExecutionEnvironment env, Configuration configuration, DataStream<StringObjectMap> inputStream, Class<Configuration> aClass) {

        // Make sure we have a good configuration
        configuration.validate();

        // Step 1 - Key them by idempotency key
        KeyedStream<StringObjectMap, String> keyedStream = inputStream.keyBy(new KeySelectorImpl());

        // Step 2 - Process them i.e. put them in delay queue
        SingleOutputStreamOperator<StringObjectMap> processedStream = keyedStream.process(new CustomProcessor(configuration))
                .name("processed-stream").uid("d16904b4-3524-11ed-a261-0242ac120002");

        // Step 3.1 - Send it to store
        KafkaSinkConfig kafkaSinkConfig = configuration.getKafkaSinks().get("mainOutput");
        FlinkKafkaProducer<StringObjectMap> sink = kafkaSinkConfig.getKafkaSinkWithStringObjectMap(env, new ObjectToKeyConvertorImpl());
        processedStream.addSink(sink).name("kafka-sink").uid("d169059a-3524-11ed-a261-0242ac120002");

        // Step 3.2 - Send it to store
        if (configuration.getMiscellaneousProperties().getBoolean("console-debug-sink-enabled")) {
            processedStream.addSink(new DebugSync<>()).name("debug-sink").uid("d169066c-3524-11ed-a261-0242ac120002");
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
        String jobName = "MissedEventJobV2";
        for (int i = 0; i < args.length; i++) {
            if (Objects.equal(args[i], "--name")) {
                jobName = args[i + 1];
                break;
            }
        }

        Main job = new Main();
        MainTemplateV2<Configuration> template = new MainTemplateV2<>();
        template.main(args, jobName, job, Configuration.class);
    }

    // Key selector - to partition data
    private static final class KeySelectorImpl implements KeySelector<StringObjectMap, String>, Serializable {
        @Override
        public String getKey(StringObjectMap value) {
            if (value != null && value.containsKey(ID_PARAM_NAME)) {
                if (Strings.isNullOrEmpty(value.getString(ID_PARAM_NAME))) {
                    return UUID.randomUUID().toString();
                } else {
                    return value.getString(ID_PARAM_NAME);
                }
            } else {
                return UUID.randomUUID().toString();
            }
        }
    }

    // Key selector - to partition data
    public static final class ObjectToKeyConvertorImpl implements KafkaSourceHelper.ObjectToKeyConvertor<StringObjectMap>, Serializable {
        private final KeySelectorImpl keySelector = new KeySelectorImpl();

        private String keyAsString(StringObjectMap value) {
            return keySelector.getKey(value);
        }

        @Override
        public byte[] key(StringObjectMap value) {
            return keyAsString(value).getBytes();
        }

        @Override
        public byte[] getKey(StringObjectMap value) {
            return keyAsString(value).getBytes();
        }

        @Override
        public int partition(StringObjectMap value, byte[] bytes, byte[] bytes1, String s, int[] partitions) {
            String key = keyAsString(value);
            return Math.abs(key.hashCode() % partitions.length);
        }
    }
}
