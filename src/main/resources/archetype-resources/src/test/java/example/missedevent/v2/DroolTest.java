#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent.v2;

import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;

public class DroolTest {
    private TestDroolsLogic droolsLogic;
    public DateTime now;
    public Configuration configuration;

    @BeforeEach
    public void setup() {
        droolsLogic = new TestDroolsLogic();
        droolsLogic.now = CalendarUtils.createTime(CalendarUtils.DATETIME_FORMAT_V1, "2022-09-06T01:08:56.235+0530");
        droolsLogic.configuration = new Configuration();
        droolsLogic.configuration.getMiscellaneousProperties().put(
                "debug-drools-print-result-filter-input-stream", true,
                "debug-drools-print-result-state-keys-func", true,
                "debug-drools-print-result-initial-event-trigger", true
        );
        now = droolsLogic.now;
        configuration = droolsLogic.configuration;
    }

    @Test
    public void testOnEventOnReceived_FilterNotNeededEvents() {

        StringObjectMap appleOrder = StringObjectMap.of(
                "user_id", "1234",
                "timestamp", now.getMillis(),
                "idempotency", UUID.randomUUID().toString(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        System.out.println(JsonUtils.asJson(appleOrder));

        ResultMap resultMap = new ResultMap();
        droolsLogic.onEventReceived(appleOrder, resultMap, configuration);
        Assertions.assertEquals(false, resultMap.getBoolean("skip"));
        Assertions.assertEquals("1234", resultMap.getString("group-key"));

        // Skip this event
        appleOrder = StringObjectMap.of(
                "user_id", "1234",
                "timestamp", now,
                "idempotency", UUID.randomUUID().toString(),
                "data", StringObjectMap.of(
                        "order_status", "NOT_COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        resultMap = new ResultMap();
        droolsLogic.onEventReceived(appleOrder, resultMap, configuration);
        Assertions.assertEquals(true, resultMap.getBoolean("skip"));
    }

    @Test
    public void testAfterEventIsFiltered_AndGoodToProcess() {

        // Event 1 - with phone=apple
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", UUID.randomUUID().toString(),
                "timestamp", now.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "INIT",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        ResultMap resultMap = new ResultMap();

        // Event 1 - INIT where we must retain a state
        droolsLogic.onEventAfterFilter(appleOrder, resultMap, configuration);

        Map<KeyPair, StringObjectMap> retainObjects = resultMap.getMap("retain-objects", KeyPair.class, StringObjectMap.class);
        Assertions.assertNotNull(retainObjects);
        Assertions.assertEquals(1, retainObjects.size());
        Assertions.assertTrue(resultMap.getBoolean("retain-state"));
        Assertions.assertEquals("1234", retainObjects.get(new KeyPair("1234", "na")).getStringObjectMap("data").get("user_id"));
        Assertions.assertEquals("order_1", retainObjects.get(new KeyPair("1234", "na")).getStringObjectMap("data").get("order_id"));

        // Event 2 - COMPLETED where we must delete retained state
        appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "idempotency", UUID.randomUUID().toString(),
                "timestamp", now.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        resultMap = new ResultMap();
        droolsLogic.onEventAfterFilter(appleOrder, resultMap, configuration);
        retainObjects = resultMap.getMap("retain-objects", KeyPair.class, StringObjectMap.class);
        Assertions.assertNull(retainObjects);
        Assertions.assertTrue(resultMap.getBoolean("delete-retained-state"));
    }
}
