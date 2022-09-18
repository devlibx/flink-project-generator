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
import io.github.devlibx.miscellaneous.flink.drools.IRuleEngineProvider;
import io.github.devlibx.miscellaneous.flink.store.GenericState;
import io.github.devlibx.miscellaneous.flink.store.IGenericStateStore;
import io.github.devlibx.miscellaneous.flink.store.ProxyBackedGenericStateStore;
import io.github.devlibx.miscellaneous.util.aggregation.TimeWindowDataAggregation;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Slf4j
public class CustomProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {
    private final IRuleEngineProvider ruleEngineProvider;
    private transient MapState<String, TimeWindowDataAggregation> mapState;
    private transient MapState<String, String> idempotenceState;
    private final Configuration configuration;

    @Setter
    @Getter
    private IGenericStateStore genericStateStore;

    public CustomProcessor(IRuleEngineProvider ruleEngineProvider, Configuration configuration) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.configuration = configuration;
        this.genericStateStore = new ProxyBackedGenericStateStore(configuration);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(configuration.getTtl().get("main-processor-state-ttl", "ttl", Integer.class)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, TimeWindowDataAggregation> mapStateDescriptor = new MapStateDescriptor<>(
                "main-processor-state",
                String.class,
                TimeWindowDataAggregation.class
        );
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);

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

        // Get an existing state
        ExistingState states = null;
        try {
            states = getExistingState(value);
        } catch (DuplicateEventException e) {
            return; // If this is a duplicate event then ignore
        }

        // Make a new session - we will mark agenda-group to run selected rules
        ResultMap resultMap = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("initial-event-trigger", value, resultMap, states, configuration);

        // See if we should retain state
        if (resultMap.getBoolean("retain-state", true)) {
            retainStateToFlinkState(resultMap);
        }

        // If skip is set to true then we will not send it out
        if (!resultMap.getBoolean("skip", false)) {
            forwardObjects(resultMap, out);
        }
    }

    /**
     * This method will save the state to the flink state - these states are provided by rule engine to store in flink
     * state
     */
    private void retainStateToFlinkState(ResultMap result) throws Exception {

        // Get retain objects
        Map<KeyPair, StringObjectMap> retainObjects = (Map<KeyPair, StringObjectMap>) result.get("retain-objects");
        if (retainObjects == null) {
            log.warn("retain state cannot be done, retain-objects is null");
            return;
        }

        // Save all states with given keys - these keys and state are returned from rule engine
        for (KeyPair key : retainObjects.keySet()) {
            StringObjectMap stateToSaveMap = retainObjects.get(key);
            if (stateToSaveMap == null) {
                log.warn("retain state cannot be done, retain-object is not set for key={}", key);
            } else if (!(stateToSaveMap.get("aggregation") instanceof TimeWindowDataAggregation)) {
                log.warn("retain state cannot be done, retain-object is set but aggregation object is missing in key={}", key);
            } else {
                TimeWindowDataAggregation stateToSave = stateToSaveMap.get("aggregation", TimeWindowDataAggregation.class);
                mapState.put(key.compiledStringKey(), stateToSave);
            }
        }
    }

    private void forwardObjects(ResultMap result, Collector<StringObjectMap> out) throws Exception {
        Map<KeyPair, StringObjectMap> forwardObjects = result.getMap("forward-objects", KeyPair.class, StringObjectMap.class);
        if (forwardObjects == null) {
            log.warn("retain state cannot be done, forward-objects is null");
        } else {
            for (KeyPair key : forwardObjects.keySet()) {
                out.collect(forwardObjects.get(key));
            }
        }
    }

    private ExistingState getExistingState(StringObjectMap value) throws Exception {
        ExistingState states = new ExistingState();

        // Run rule engine and ask for the state keys to fetch
        ResultMap result = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("initial-event-trigger-get-state-to-fetch", result, value, configuration);

        // Ignore duplicate events
        if (result.containsKey("idempotency-key") && result.get("idempotency-key") instanceof String) {
            if (idempotenceState.contains(result.getString("idempotency-key"))) {
                throw new DuplicateEventException("Duplicate event - ignore this event");
            } else {
                idempotenceState.put(result.getString("idempotency-key"), "");
            }
        }

        List<KeyPair> stateKeysToFetch = result.getList("states-to-provide", KeyPair.class);
        if (stateKeysToFetch == null) {
            log.warn("states-to-provide is missing from rule engine output (check your logic of 'initial-event-trigger-get-state-to-fetch' section in rule file)");
            return states;
        }

        for (KeyPair key : stateKeysToFetch) {

            // Get from state - otherwise from store
            TimeWindowDataAggregation existingState = null;
            if (mapState.contains(key.compiledStringKey())) {
                existingState = mapState.get(key.compiledStringKey());
            } else {
                GenericState state = genericStateStore.get(key.buildKey());
                if (state != null) {
                    existingState = JsonUtils.readObject(JsonUtils.asJson(state.getData()), TimeWindowDataAggregation.class);
                }
            }

            // Give back this state to called
            if (existingState != null) {
                states.put(
                        key.compiledStringKey(),
                        StringObjectMap.of("aggregation", existingState)
                );
            } else {
                states.put(
                        key.compiledStringKey(),
                        StringObjectMap.of("aggregation", new TimeWindowDataAggregation())
                );
            }
        }

        return states;
    }

    // Duplicate event
    public static final class DuplicateEventException extends RuntimeException implements Serializable {
        public DuplicateEventException(String error) {
            super(error);
        }
    }
}
