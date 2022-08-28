#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.missedevent;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.rule.drools.DroolsHelper;
import io.github.devlibx.easy.rule.drools.ResultMap;
import ${package}.example.drools.IRuleEngineProvider;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.runtime.KieSession;

public class CustomProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {

    private final IRuleEngineProvider ruleEngineProvider;
    private transient MapState<String, StringObjectMap> mapState;
    private final int ttl;

    public CustomProcessor(IRuleEngineProvider ruleEngineProvider, int ttl) {
        this.ruleEngineProvider = ruleEngineProvider;
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(ttl))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, StringObjectMap> mapStateDescriptor = new MapStateDescriptor<>(
                "map",
                String.class,
                StringObjectMap.class
        );
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx, Collector<StringObjectMap> out) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        DroolsHelper droolsHelper = ruleEngineProvider.getDroolsHelper();
        KieSession kSession = droolsHelper.getKieSessionWithAgenda("initial-event-trigger");
        ResultMap result = new ResultMap();
        kSession.insert(result);
        kSession.insert(value);
        kSession.fireAllRules();

        // Check we should retain this object in state
        boolean retainState = result.getBoolean("retain-state", true);
        if (retainState) {
            saveState(result, value, ctx);
        } else {
            boolean deleteRetainState = result.getBoolean("retain-state-delete", true);
            if (deleteRetainState) {
                deleteRetainedState(result, value, ctx);
            }
        }
    }

    private void saveState(ResultMap result, StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx) throws Exception {

        // Make sure we have retained key
        String key = result.getString("retain-state-key", "");
        if (Strings.isNullOrEmpty(key)) {
            System.out.println("------>>> WARN: retain-state-key is not provided: " + JsonUtils.asJson(result));
            return;
        }

        // Make sure we have retained object
        StringObjectMap objectToStore = result.getStringObjectMap("retain-object");
        if (objectToStore == null) {
            System.out.println("------>>> WARN: retain-object is not provided: " + JsonUtils.asJson(result));
            return;
        }

        // Set up a timer for "waitForSec" sec - we will get called after "waitForSec" sec
        int waitForSec = result.getInt("retain-state-expiry-in-sec", 30);
        long timer = ctx.timerService().currentProcessingTime() + ((long) waitForSec * 1000);
        ctx.timerService().registerProcessingTimeTimer(timer);

        // Persist this object in state
        mapState.put(key, objectToStore);
    }

    private void deleteRetainedState(ResultMap result, StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx) throws Exception {
        // Make sure we have retained key
        String key = result.getString("retain-state-key", "");
        if (Strings.isNullOrEmpty(key)) {
            System.out.println("------>>> WARN: retain-state-key is not provided to delete: " + JsonUtils.asJson(result));
            return;
        }

        // Delete the state - we got the complete event
        mapState.remove(key);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.OnTimerContext ctx, Collector<StringObjectMap> out) throws Exception {

        // Get the object with key and also delete it from map state
        StringObjectMap storedState = mapState.get(ctx.getCurrentKey());
        if (storedState == null) {
            return;
        }
        mapState.remove(ctx.getCurrentKey());

        // Make a new session - we will mark agenda-group to run selected rules
        DroolsHelper droolsHelper = ruleEngineProvider.getDroolsHelper();
        KieSession kSession = droolsHelper.getKieSessionWithAgenda("expiry-event-trigger");
        ResultMap result = new ResultMap();
        kSession.insert(result);
        kSession.insert(storedState);
        kSession.fireAllRules();

        // Trigger event
        boolean triggerEvent = result.getBoolean("trigger-expiry", false);
        if (triggerEvent) {
            StringObjectMap objectToTrigger = result.getStringObjectMap("trigger-object");
            out.collect(objectToTrigger);
        }
    }
}
