#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;

import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import io.github.devlibx.miscellaneous.flink.drools.IRuleEngineProvider;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static ${package}.example.aggregation.DroolTest.primaryKeyPrefix;
import static ${package}.example.aggregation.DroolTest.secondaryKeyPrefix;

public class CustomProcessorTest {
    private final DateTime timeToUse = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-07-05T17:26:35.302+05:30");

    @Test
    public void testProcessFunction() throws Exception {
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(
                "jar:///test_aggregate_sample_rule.drl"
        );

        Configuration configuration = new Configuration();
        configuration.getMiscellaneousProperties().put(
                "debug-drools-print-result-filter-input-stream", true,
                "debug-drools-print-result-state-keys-func", true,
                "debug-drools-print-result-initial-event-trigger", true
        );
        configuration.getTtl().put("main-processor-state-ttl", StringObjectMap.of("ttl", 10));

        // Create function to test
        IGenericStateStore store = Mockito.mock(IGenericStateStore.class);
        CustomProcessor processFunction = new CustomProcessor(
                ruleEngineProvider,
                configuration
        );
        processFunction.setGenericStateStore(store);

        String idempotency = UUID.randomUUID().toString();
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", idempotency,
                "timestamp", timeToUse.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );

        // Create harness
        OneInputStreamOperatorTestHarness<StringObjectMap, StringObjectMap> harness = ProcessFunctionTestHarnesses.forKeyedProcessFunction(
                processFunction,
                value -> value.getString("user_id"),
                Types.STRING
        );

        harness.processElement(appleOrder, timeToUse.getMillis());

        // Send duplicate events - should not process
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());


        // Event 2 - with phone=samsung
        StringObjectMap samsungOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "timestamp", timeToUse.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Samsung",
                        "category", "phone"
                )
        );
        harness.processElement(samsungOrder, timeToUse.getMillis());

        List<StreamRecord<? extends StringObjectMap>> output = harness.extractOutputStreamRecords();
        Assertions.assertEquals(4, output.size());
        System.out.println("---------- Stream out ----------");
        output.forEach(streamRecord -> {
            System.out.println("Stream Out = " + JsonUtils.asJson(streamRecord));

        });

        String naSubKey = secondaryKeyPrefix + "na";
        String phoneSubKey = secondaryKeyPrefix + "phone";

        StringObjectMap out = output.get(0).getValue();
        System.out.println(JsonUtils.asJson(out));
        Assertions.assertEquals(primaryKeyPrefix + "1234", out.get("key_pair", KeyPair.class).getKey());
        Assertions.assertTrue(Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), naSubKey)
                || Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), phoneSubKey));

        out = output.get(1).getValue();
        Assertions.assertEquals(primaryKeyPrefix + "1234", out.get("key_pair", KeyPair.class).getKey());
        System.out.println(JsonUtils.asJson(out));
        Assertions.assertTrue(Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), naSubKey)
                || Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), phoneSubKey));

        out = output.get(2).getValue();
        Assertions.assertEquals(primaryKeyPrefix + "1234", out.get("key_pair", KeyPair.class).getKey());
        Assertions.assertTrue(Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), naSubKey)
                || Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), phoneSubKey));

        out = output.get(3).getValue();
        Assertions.assertEquals(primaryKeyPrefix + "1234", out.get("key_pair", KeyPair.class).getKey());
        Assertions.assertTrue(Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), naSubKey)
                || Objects.equals(out.get("key_pair", KeyPair.class).getSubKey(), phoneSubKey));
    }
}
