#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent.v2;

import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.miscellaneous.flink.drools.IRuleEngineProvider;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class CustomProcessorTest {
    private final DateTime timeToUse = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-07-05T17:26:35.302+05:30");


    @BeforeEach
    public void setup() throws IOException {
        CodeGeneratorTest generatorTest = new CodeGeneratorTest();
        generatorTest.generateDrlFile();
    }

    @Test
    public void testProcessFunction_WhenTimerDidNotExpired() throws Exception {
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(
                "jar:///test_missedevent_v2_rule.drl"
        );

        Configuration configuration = new Configuration();
        configuration.getMiscellaneousProperties().put(
                "debug-drools-print-result-filter-input-stream", true,
                "debug-drools-print-result-state-keys-func", true,
                "debug-drools-print-result-initial-event-trigger", true
        );
        configuration.getTtl().put("main-processor-state-ttl", StringObjectMap.of("ttl", 10));

        CustomProcessor processFunction = new CustomProcessor(ruleEngineProvider, configuration);

        String idempotency = UUID.randomUUID().toString();
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", idempotency,
                "timestamp", timeToUse.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "INIT",
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
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        // Send duplicate events - should not process
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        // harness.setProcessingTime(timeToUse.plusSeconds(15).getMillis());

        // We must not get any alert events
        List<StreamRecord<? extends StringObjectMap>> output = harness.extractOutputStreamRecords();
        Assertions.assertEquals(0, output.size());
    }

    @Test
    public void testProcessFunction_WhenTimerExpired() throws Exception {
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(
                "jar:///test_missedevent_v2_rule.drl"
        );

        Configuration configuration = new Configuration();
        configuration.getMiscellaneousProperties().put(
                "debug-drools-print-result-filter-input-stream", true,
                "debug-drools-print-result-state-keys-func", true,
                "debug-drools-print-result-initial-event-trigger", true
        );
        configuration.getTtl().put("main-processor-state-ttl", StringObjectMap.of("ttl", 10));

        CustomProcessor processFunction = new CustomProcessor(ruleEngineProvider, configuration);

        String idempotency = UUID.randomUUID().toString();
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", idempotency,
                "timestamp", timeToUse.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "INIT",
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
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        // Send duplicate events - should not process
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        // Move time so we get onTimer() event
        harness.setProcessingTime(timeToUse.plusSeconds(15).getMillis());

        // We must not get any alert events
        List<StreamRecord<? extends StringObjectMap>> output = harness.extractOutputStreamRecords();
        Assertions.assertEquals(1, output.size());
        output.forEach(streamRecord -> {
            System.out.println("Stream Out = " + JsonUtils.asJson(streamRecord));
        });

        StringObjectMap out = output.get(0).getValue();
        System.out.println(JsonUtils.asJson(out));
        Assertions.assertEquals("1234", out.get("key"));
        Assertions.assertEquals("na", out.get("sub_key"));
        Assertions.assertEquals("1234", out.getStringObjectMap("data").get("user_id"));
        Assertions.assertEquals("order_1", out.getStringObjectMap("data").get("order_id"));
    }

    @Test
    public void testProcessFunction_WhenTimerExpired_ButWeAlreadyGotCompleted() throws Exception {
        IRuleEngineProvider ruleEngineProvider = new IRuleEngineProvider.ProxyDroolsHelper(
                "jar:///test_missedevent_v2_rule.drl"
        );

        Configuration configuration = new Configuration();
        configuration.getMiscellaneousProperties().put(
                "debug-drools-print-result-filter-input-stream", true,
                "debug-drools-print-result-state-keys-func", true,
                "debug-drools-print-result-initial-event-trigger", true
        );
        configuration.getTtl().put("main-processor-state-ttl", StringObjectMap.of("ttl", 10));

        CustomProcessor processFunction = new CustomProcessor(ruleEngineProvider, configuration);

        String idempotency = UUID.randomUUID().toString();
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", idempotency,
                "timestamp", timeToUse.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "INIT",
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
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        // Send duplicate events - should not process
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        harness.processElement(appleOrder, timeToUse.getMillis());
        Assertions.assertEquals(1, harness.numProcessingTimeTimers());

        idempotency = UUID.randomUUID().toString();
        appleOrder = StringObjectMap.of(
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
        harness.processElement(appleOrder, timeToUse.plusSeconds(1).getMillis());

        // Move time so we get onTimer() event
        harness.setProcessingTime(timeToUse.plusSeconds(15).getMillis());

        // We must not get any alert events
        List<StreamRecord<? extends StringObjectMap>> output = harness.extractOutputStreamRecords();
        Assertions.assertEquals(0, output.size());
    }
}
