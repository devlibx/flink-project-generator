#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import io.github.devlibx.miscellaneous.flink.drools.ExistingState;
import io.github.devlibx.miscellaneous.util.aggregation.CustomAggregationUpdater;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregation;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregationHelper.IAggregationUpdater;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestDroolsLogic {
    public DateTime now;
    public Configuration configuration;


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
        final String primaryIdKey = "user_id";
        final List<String> validStatusList = Arrays.asList("COMPLETED", "INIT");
        final String debugKey = "debug-drools-print-result-filter-input-stream";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        String status = event.get("data", "order_status", String.class);
        String primaryId = event.get(primaryIdKey, String.class);
        if (validStatusList.contains(status)) {
            resultMap.put("skip", false);
            resultMap.put("group-key", primaryId);
        } else {
            resultMap.put("skip", true);
        }

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println(JsonUtils.asJson(resultMap));
        }
    }

    /**
     * <pre>
     * rule "Fetch State Keys"
     *   dialect "java"
     *   agenda-group "initial-event-trigger-get-state-to-fetch"
     *   when
     *       event : StringObjectMap()
     *       resultMap: ResultMap()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onEventProcessing_FetchStateKeys(StringObjectMap event, ResultMap resultMap, Configuration configuration) {
        // Constants - change this for your use case
        final String primaryKeyPrefix = "user_case_1_pk${symbol_pound}";
        final String secondaryKeyPrefix = "user_case_1_sk${symbol_pound}";

        final String primaryIdKey = "user_id";
        final String debugKey = "debug-drools-print-result-state-keys-func";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        String primaryId = primaryKeyPrefix + event.get(primaryIdKey, String.class);
        String secondaryId = secondaryKeyPrefix + event.get("data", "category", String.class);
        resultMap.put("states-to-provide", Arrays.asList(new KeyPair(primaryId, "na"), new KeyPair(primaryId, secondaryId)));

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println(JsonUtils.asJson(resultMap));
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
     *       existingState : ExistingState()
     *       configuration: Configuration()
     *   then
     * </pre>
     */
    public void onEventAfterFilter(StringObjectMap event, ExistingState existingState, ResultMap resultMap, Configuration configuration) {
        // Constants - change this for your use case
        final String primaryKeyPrefix = "user_case_1_pk${symbol_pound}";
        final String secondaryKeyPrefix = "user_case_1_sk${symbol_pound}";
        final String primaryIdKey = "user_id";
        final String debugKey = "debug-drools-print-result-initial-event-trigger";
        final StringObjectMap miscellaneousProperties = configuration.getMiscellaneousProperties();

        DateTime eventTime = event.getDateTimeFromMilesOrDefault("timestamp", DateTime.now());
        DateTime now = DateTime.now();
        if (event.getBoolean("is_test", false)) {
            now = eventTime;
        }

        final String primaryId = primaryKeyPrefix + event.get(primaryIdKey, String.class);
        final String secondaryKey = secondaryKeyPrefix + event.get("data", "category", String.class);
        final KeyPair primaryIdKeyPair = new KeyPair(primaryId, "na");
        final KeyPair primaryAndSecondaryIdKeyPair = new KeyPair(primaryId, secondaryKey);

        // ************************ Aggregation 1 - Count orders done by user ******************************************
        TimeWindowDataAggregation aggregationOrdersByUser = existingState.get(primaryIdKeyPair, "aggregation", TimeWindowDataAggregation.class);
        TimeWindowDataAggregationHelper<StringObjectMap> aggregationOrdersByUserHelper = new TimeWindowDataAggregationHelper<>(
                TimeWindowDataAggregationHelper.Config.builder()
                        .dayAggregationWindow(31)
                        .hourAggregationWindow(24)
                        .minuteAggregationWindow(60)
                        .build()
        );
        IAggregationUpdater<StringObjectMap> updater = new CustomAggregationUpdater.IncrementCounter();
        aggregationOrdersByUserHelper.process(aggregationOrdersByUser, now, event, eventTime, updater);
        System.out.println("aggregationOrdersByUser:" + JsonUtils.asJson(aggregationOrdersByUser));

        // ************************ Aggregation 2 - Count merchants used by user ***************************************
        TimeWindowDataAggregation aggregationOfMerchantsUserUsed = existingState.get(primaryAndSecondaryIdKeyPair, "aggregation", TimeWindowDataAggregation.class);
        TimeWindowDataAggregationHelper<StringObjectMap> aggregationOfMerchantsUserHelper = new TimeWindowDataAggregationHelper<>(
                TimeWindowDataAggregationHelper.Config.builder()
                        .dayAggregationWindow(7)
                        .build()
        );
        updater = new CustomAggregationUpdater.StringAppender(event.get("data", "merchant_id", String.class));
        aggregationOfMerchantsUserHelper.process(aggregationOfMerchantsUserUsed, now, event, eventTime, updater);
        System.out.println("aggregationOfMerchantsUserUsed:" + JsonUtils.asJson(aggregationOfMerchantsUserUsed));

        // What to retain in state
        Map<KeyPair, StringObjectMap> retainObjects = new HashMap<>();
        retainObjects.put(primaryIdKeyPair, StringObjectMap.of("aggregation", aggregationOrdersByUser));
        retainObjects.put(primaryAndSecondaryIdKeyPair, StringObjectMap.of("aggregation", aggregationOfMerchantsUserUsed));

        Map<KeyPair, StringObjectMap> forwardObjects = new HashMap<>();
        forwardObjects.put(primaryIdKeyPair, StringObjectMap.of(
                "key_pair", primaryIdKeyPair,
                "aggregation", aggregationOrdersByUser
        ));
        forwardObjects.put(primaryAndSecondaryIdKeyPair, StringObjectMap.of(
                "key_pair", primaryAndSecondaryIdKeyPair,
                "aggregation", aggregationOfMerchantsUserUsed
        ));

        resultMap.put("retain-state", true);
        resultMap.put("retain-objects", retainObjects);
        resultMap.put("forward-objects", forwardObjects);

        if (miscellaneousProperties.containsKey(debugKey) && miscellaneousProperties.get(debugKey, Boolean.class)) {
            System.out.println("Result Map:" + JsonUtils.asJson(resultMap));
        }
    }
}
