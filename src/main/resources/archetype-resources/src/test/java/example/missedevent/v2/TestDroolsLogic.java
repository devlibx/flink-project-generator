#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent.v2;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import ${package}.example.missedevent.v2.CustomProcessor.InternalData;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestDroolsLogic {
    public transient DateTime now;
    public transient Configuration configuration;

    public static final String primaryIdKey = "user_id";
    public static final String secondaryIdKey = "data.order_status";

    /**
     * <pre>
     * rule "Filter Input Stream"
     *   dialect "java"
     *   agenda-group "filter-input-stream"
     * when
     * event : StringObjectMap()
     * resultMap: ResultMap()
     * configuration: Configuration()
     * then
     * </pre>
     */
    public void onEventReceived(StringObjectMap event, ResultMap resultMap, Configuration configuration) {
        final List<String> validStatusList = Arrays.asList("COMPLETED", "INIT");
        final String debugKey = "debug-drools-print-result-filter-input-stream";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        String status = event.path(secondaryIdKey, String.class);
        String primaryId = event.path(primaryIdKey, String.class);
        if (validStatusList.contains(status)) {
            resultMap.put("skip", false);
            resultMap.put("group-key", primaryId);
        } else {
            resultMap.put("skip", true);
        }

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println("Drools Filter Input Event Result = " + JsonUtils.asJson(resultMap));
        }
    }

    /**
     * <pre>
     * rule "Order Initiated"
     *   dialect "java"
     *   agenda-group "initial-event-trigger"
     *   when
     *       event : StringObjectMap()
     *       resultMap: ResultMap()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onEventAfterFilter(StringObjectMap event, ResultMap resultMap, Configuration configuration) {
        // Constants - change this for your use case
        final String debugKey = "debug-drools-print-result-initial-event-trigger";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        resultMap.put("idempotency-key", event.get("idempotency"));

        if (Objects.equals(event.path(secondaryIdKey, String.class), "INIT")) {

            // Wait for 10 sec after alert tha completed event did not come
            resultMap.put("time-to-wait-for-other-event-in-sec", 5);

            // When we did not get the event on time then we will forward this as alert
            Map<KeyPair, StringObjectMap> retainObjects = new HashMap<>();
            retainObjects.put(
                    new KeyPair(
                            event.getString("user_id"),
                            "na"
                    ),
                    StringObjectMap.of(
                            "data",
                            StringObjectMap.of(
                                    "user_id", event.getString("user_id"),
                                    "order_id", event.get("data", "order_id", String.class)
                            )
                    )

            );
            resultMap.put("retain-objects", retainObjects);

            // Make sure we retain data
            resultMap.put("retain-state", true);

        } else if (Objects.equals(event.path(secondaryIdKey, String.class), "COMPLETED")) {

            // Got completed event, delete the state
            resultMap.put("retain-state", false);
            resultMap.put("delete-retained-state", true);
        } else {
            resultMap.put("skip", true);
        }
    }

    /**
     * <pre>
     * rule "Did not get completed event"
     *   dialect "java"
     *   agenda-group "expiry-event-trigger"
     *   when
     *       storedState : StringObjectMap()
     *       resultMap: ResultMap()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onExpiry(ResultMap resultMap, InternalData storedState, Configuration configuration) {
        resultMap.put("expiry-event-trigger-execute-default", true);

        // NOTE - If you pass this a "false" then you must also implement "onExpiryGetPartitionKey" to give
        // proper key
    }

    /**
     * <pre>
     * rule "Get the partition key for output sink"
     *   dialect "java"
     *   agenda-group "expiry-event-trigger-partition-key"
     *   when
     *       objectToBeEmitted : StringObjectMap()
     *       resultMap: ResultMap()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onExpiryGetPartitionKey(StringObjectMap objectToBeEmitted, ResultMap resultMap, Configuration configuration) {
        final String debugKey = "debug-drools-print-result-expiry-event-trigger-partition-key";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        // NOTE - this is OK if expiry-event-trigger-execute-default=true. If it is set to false then you must
        // generate the partition-key from input "objectToBeEmitted"

        resultMap.put("partition-key", objectToBeEmitted.getString("key") + "${symbol_pound}" + objectToBeEmitted.get("sub_key"));

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println("Drools Kafka Sink Partition Key Result = " + JsonUtils.asJson(resultMap));
        }
    }
}
