#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;

import io.gitbub.devlibx.easy.helper.calendar.CalendarUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import io.github.devlibx.miscellaneous.flink.drools.ExistingState;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregation;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DroolTest {
    private TestDroolsLogic droolsLogic;
    public DateTime now;
    public Configuration configuration;

    // Constants - change this for your use case
    public static final String primaryKeyPrefix = TestDroolsLogic.primaryKeyPrefix;
    public static final String secondaryKeyPrefix = TestDroolsLogic.secondaryKeyPrefix;
    public static final String primaryIdKey = TestDroolsLogic.primaryIdKey;

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
    public void testOnEventOnReceived() {

        StringObjectMap appleOrder = StringObjectMap.of(
                "user_id", "1234",
                "timestamp", now,
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        ResultMap resultMap = new ResultMap();
        droolsLogic.onEventReceived(appleOrder, resultMap, configuration);
        Assertions.assertEquals(false, resultMap.getBoolean("skip"));
        Assertions.assertEquals("1234", resultMap.getString("group-key"));

        // Skip this event
        appleOrder = StringObjectMap.of(
                "user_id", "1234",
                "timestamp", now,
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
    public void testAfterEventIsFiltered_GetStateKeys() {
        StringObjectMap appleOrder = StringObjectMap.of(
                "user_id", "1234",
                "timestamp", now,
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        ResultMap resultMap = new ResultMap();
        droolsLogic.onEventProcessing_FetchStateKeys(appleOrder, resultMap, configuration);
        System.out.println("Key 1: " + resultMap.getList("states-to-provide", KeyPair.class).get(0));
        System.out.println("Key 2: " + resultMap.getList("states-to-provide", KeyPair.class).get(1));
        Assertions.assertEquals(new KeyPair(primaryKeyPrefix + "1234", secondaryKeyPrefix + "na"), resultMap.getList("states-to-provide", KeyPair.class).get(0));
        Assertions.assertEquals(new KeyPair(primaryKeyPrefix + "1234", secondaryKeyPrefix + "phone"), resultMap.getList("states-to-provide", KeyPair.class).get(1));
    }

    @Test
    public void testAfterEventIsFiltered_EventProcessing() {


        // Event 1 - with phone=apple
        StringObjectMap appleOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "timestamp", now.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Apple",
                        "category", "phone"
                )
        );
        ResultMap resultMap = new ResultMap();

        String primaryId = primaryKeyPrefix + appleOrder.get(primaryIdKey, String.class);
        String secondaryId = secondaryKeyPrefix + appleOrder.get("data", "category", String.class);
        KeyPair primaryIdKeyPair = new KeyPair(primaryId, secondaryKeyPrefix + "na");
        KeyPair primaryAndSecondaryIdKeyPair = new KeyPair(primaryId, secondaryId);

        // Calling without any state - assume this is the first event for this user
        droolsLogic.onEventAfterFilter(appleOrder,
                ExistingState.from(
                        primaryIdKeyPair, StringObjectMap.of("aggregation", new TimeWindowDataAggregation()),
                        primaryAndSecondaryIdKeyPair, StringObjectMap.of("aggregation", new TimeWindowDataAggregation())
                ),
                resultMap,
                configuration);

        // Event 2 - with phone=samsung
        StringObjectMap samsungOrder = StringObjectMap.of(
                "is_test", true,
                "user_id", "1234",
                "timestamp", now.getMillis(),
                "data", StringObjectMap.of(
                        "order_status", "COMPLETED",
                        "order_id", "order_1",
                        "merchant_id", "Samsung",
                        "category", "phone"
                )
        );

        // Calling with state - we had received a phone=Apple event before
        ExistingState oldState = ExistingState.from(
                primaryIdKeyPair, resultMap.getMap("retain-objects", KeyPair.class, StringObjectMap.class).get(primaryIdKeyPair),
                primaryAndSecondaryIdKeyPair, resultMap.getMap("retain-objects", KeyPair.class, StringObjectMap.class).get(primaryAndSecondaryIdKeyPair)
        );
        droolsLogic.onEventAfterFilter(samsungOrder, oldState, resultMap, configuration);

        Map<KeyPair, StringObjectMap> retainObjects = resultMap.getMap("retain-objects", KeyPair.class, StringObjectMap.class);
        StringObjectMap temp = retainObjects.get(primaryIdKeyPair);
        TimeWindowDataAggregation aggregationByUser = temp.get("aggregation", TimeWindowDataAggregation.class);
        Assertions.assertEquals(2, aggregationByUser.getDays().getInt("9-6"));
        Assertions.assertEquals(2, aggregationByUser.getHours().getInt("6-1"));
        Assertions.assertEquals(2, aggregationByUser.getMinutes().getInt("1-8"));

        temp = retainObjects.get(primaryAndSecondaryIdKeyPair);
        TimeWindowDataAggregation aggregationByUserPhone = temp.get("aggregation", TimeWindowDataAggregation.class);
        Assertions.assertEquals("Apple,Samsung", aggregationByUserPhone.getDays().getString("9-6"));
    }
}
