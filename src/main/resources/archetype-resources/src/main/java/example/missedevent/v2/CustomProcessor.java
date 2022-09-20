#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent.v2;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import io.github.devlibx.miscellaneous.flink.drools.IRuleEngineProvider;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class CustomProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {
    private final IRuleEngineProvider ruleEngineProvider;
    private transient MapState<String, InternalData> eventsToBeSendOnExpiryState;
    private transient MapState<String, String> idempotenceState;
    private final Configuration configuration;

    public CustomProcessor(IRuleEngineProvider ruleEngineProvider, Configuration configuration) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.configuration = configuration;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(configuration.getTtl().get("main-processor-state-ttl", "ttl", Integer.class)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, InternalData> mapStateDescriptor = new MapStateDescriptor<>(
                "main-processor-state",
                String.class,
                InternalData.class
        );
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        eventsToBeSendOnExpiryState = getRuntimeContext().getMapState(mapStateDescriptor);

        int idempotencyTTL = 86400;
        try {
            idempotencyTTL = configuration.getTtl().get("main-processor-state-idempotency-ttl", "ttl", Integer.class);
        } catch (Exception e) {
            log.error("Did not find TTL for Idempotency - using default of 1 Day");
        }
        StateTtlConfig idempotenceTtlConfig = StateTtlConfig
                .newBuilder(Time.seconds(idempotencyTTL))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, String> idempotenceStateDescriptor = new MapStateDescriptor<>(
                "main-processor-idempotence-state",
                String.class,
                String.class
        );
        idempotenceStateDescriptor.enableTimeToLive(idempotenceTtlConfig);
        idempotenceState = getRuntimeContext().getMapState(idempotenceStateDescriptor);
    }

    @Override
    public void processElement(StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx, Collector<StringObjectMap> out) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        ResultMap resultMap = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("initial-event-trigger", value, resultMap, configuration);

        // Skip processing if required
        if (Objects.equals(resultMap.get("skip"), true)) {
            return;
        }

        // Ignore duplicate events
        if (resultMap.containsKey("idempotency-key") && resultMap.get("idempotency-key") instanceof String) {
            if (idempotenceState.contains(resultMap.getString("idempotency-key"))) {
                return;
            } else {
                if (!Strings.isNullOrEmpty(resultMap.getString("idempotency-key"))) {
                    idempotenceState.put(resultMap.getString("idempotency-key"), "");
                }
            }
        }

        // See if we should retain state
        if (resultMap.getBoolean("retain-state", true)) {

            // Save state - this will be sent if we did not get event on time
            retainStateToFlinkState(resultMap, ctx);

            // Set up a timer for "waitForSec" sec - we will get called after "waitForSec" sec
            int waitForSec = resultMap.getInt("time-to-wait-for-other-event-in-sec", 30);
            long timer = ctx.timerService().currentProcessingTime() + ((long) waitForSec * 1000);
            ctx.timerService().registerProcessingTimeTimer(timer);

        } else if (resultMap.getBoolean("delete-retained-state", true)) {

            // This will be called when the "other event is received"
            eventsToBeSendOnExpiryState.remove(ctx.getCurrentKey());
        }

    }

    /**
     * This method will save the state to the flink state - these states are provided by rule engine to store in flink
     * state
     */
    private void retainStateToFlinkState(ResultMap result, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx) throws Exception {

        // Get retain objects
        Map<KeyPair, StringObjectMap> retainObjects = result.getMap("retain-objects", KeyPair.class, StringObjectMap.class);
        if (retainObjects == null) {
            log.warn("retain state cannot be done, retain-objects is null");
            return;
        }

        // Save all states with given keys - these keys and state are returned from rule engine
        InternalData objectToRetain = new InternalData();
        for (KeyPair key : retainObjects.keySet()) {
            StringObjectMap stateToSaveMap = retainObjects.get(key);
            if (stateToSaveMap == null) {
                log.warn("retain state cannot be done, retain-object is not set for key={}", key);
            } else if (!(stateToSaveMap.get("data") instanceof StringObjectMap)) {
                log.warn("retain state cannot be done, retain-object is set but data object is missing in key={} or it is not type of StringObjectMap", key);
            } else {
                StringObjectMap stateToSave = stateToSaveMap.get("data", StringObjectMap.class);
                objectToRetain.getData().put(key, stateToSave);
            }
        }

        if (!objectToRetain.getData().isEmpty()) {
            eventsToBeSendOnExpiryState.put(ctx.getCurrentKey(), objectToRetain);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.OnTimerContext ctx, Collector<StringObjectMap> out) throws Exception {

        // Get the object with key and also delete it from map state
        InternalData storedState = eventsToBeSendOnExpiryState.get(ctx.getCurrentKey());
        if (storedState == null) {
            return;
        }
        eventsToBeSendOnExpiryState.remove(ctx.getCurrentKey());

        // Make a new session - we will mark agenda-group to run selected rules
        ResultMap result = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("expiry-event-trigger", result, storedState, configuration);

        // Forward data which has to be sent of timer expired
        if (result.getBoolean("expiry-event-trigger-execute-default", true)) {
            for (KeyPair key : storedState.getData().keySet()) {
                out.collect(StringObjectMap.of(
                        "key", key.getKey(),
                        "sub_key", key.getSubKey(),
                        "data", storedState.getData().get(key)
                ));
            }
        } else {
            Map<KeyPair, StringObjectMap> forwardObjects = result.getMap("forward-objects", KeyPair.class, StringObjectMap.class);
            if (forwardObjects == null) {
                log.warn("forward cannot be done, forward-objects is null");
            } else {
                for (KeyPair key : forwardObjects.keySet()) {
                    out.collect(forwardObjects.get(key));
                }
            }
        }
    }

    @Data
    @NoArgsConstructor

    public static class InternalData implements Serializable {
        private final Map<KeyPair, StringObjectMap> data = new HashMap<>();
    }
}
