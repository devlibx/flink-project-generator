#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.gitbub.devlibx.easy.helper.yaml.YamlUtils;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.ProxyBackedGenericStateStore;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.util.UUID;

@SuppressWarnings({"all"})
public class EndToEndTest {

    /**
     * This test will run
     */
    @Test
    @EnabledOnOs(OS.MAC)
    public void testEventProcessing_EndToEnd() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = YamlUtils.readYamlFromResourcePath("/test-aggregation-config.yaml", Configuration.class);

        // Make input stream with mock data
        String userId = UUID.randomUUID().toString();
        StringObjectMap appleOrder = StringObjectMap.of(
                "user_id", userId,
                "timestamp", DateTime.now().getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        DataStream<StringObjectMap> inputStream = env.fromElements(appleOrder);

        // Execute pipeline
        Thread mainThread = new Thread(() -> {
            Main main = new Main();
            main.internalRun(env, configuration, inputStream, Configuration.class);
            try {
                env.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        mainThread.start();

        // Verify pipeline output is written to store
        IGenericStateStore genericStateStore = new ProxyBackedGenericStateStore(configuration);
        int retries = 20;
        while (retries-- >= 0) {
            Thread.sleep(5000);
            GenericState state = genericStateStore.get(new KeyPair("user_case_1_pk${symbol_pound}" + appleOrder.getString("user_id"), "na").buildKey());
            if (state != null) {
                break;
            } else {
                System.out.println("(Must have AWS connectivity to run this test) Waiting for test to finish: pending retries = " + retries);
            }
        }

        // Store the thread after retries
        mainThread.interrupt();
    }
}
