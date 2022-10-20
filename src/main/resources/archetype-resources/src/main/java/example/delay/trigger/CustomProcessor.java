#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.delay.trigger;

import com.google.common.base.Strings;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

import static ${package}.example.delay.trigger.Main.ID_PARAM_NAME;

@Slf4j
public class CustomProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {
    private transient MapState<String, StringObjectMap> dataState;
    private transient MapState<String, String> idempotenceState;
    private final Configuration configuration;
    private final boolean debugPrintRegisterTime;
    private final boolean debugPrintOnTimer;

    public CustomProcessor(Configuration configuration) {
        this.configuration = configuration;
        debugPrintRegisterTime = configuration.getMiscellaneousProperties().getBoolean("delay-trigger-debug-register-timer", false);
        debugPrintOnTimer = configuration.getMiscellaneousProperties().getBoolean("delay-trigger-debug-on-timer", false);
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(configuration.getTtl().get("main-processor-state-ttl", "ttl", Integer.class)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        MapStateDescriptor<String, StringObjectMap> mapStateDescriptor = new MapStateDescriptor<>(
                "main-processor-state",
                String.class,
                StringObjectMap.class
        );
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        dataState = getRuntimeContext().getMapState(mapStateDescriptor);

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

        String idempotencyId = value.getString(ID_PARAM_NAME, "");
        if (Strings.isNullOrEmpty(idempotencyId) || idempotenceState.contains(idempotencyId)) {
            return;
        }

        System.out.println(JsonUtils.asJson(value));

        // Store this to state - we will send this when time expire
        dataState.put(idempotencyId, StringObjectMap.of(
                "payload", value.getStringObjectMap("payload"),
                "idempotency_id", idempotencyId
        ));

        // Setup retry timers
        long delayInMs = value.getInt("delay_sec", 60) * 1000;
        int retryCount = value.getInt("retry_count", 3);

        for (int i = 1; i <= retryCount; i++) {
            long timer = ctx.timerService().currentProcessingTime() + (i * delayInMs);
            ctx.timerService().registerProcessingTimeTimer(timer);

            if (debugPrintRegisterTime) {
                System.out.println("timer registered: id=" + idempotencyId + " time=" + new Date(timer));
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.OnTimerContext ctx, Collector<StringObjectMap> out) throws Exception {
        StringObjectMap toSend = dataState.get(ctx.getCurrentKey());
        if (toSend != null) {
            out.collect(toSend);
            if (debugPrintOnTimer) {
                System.out.println("on timer called: id=" + ctx.getCurrentKey() + " data=" + JsonUtils.asJson(toSend));
            }
        }
    }
}
