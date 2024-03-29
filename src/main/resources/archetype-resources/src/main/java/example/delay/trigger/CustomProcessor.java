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
import org.joda.time.DateTime;

import java.util.Date;
import java.util.Objects;

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

        // If idempotency id is missing then do not do anything
        String idempotencyId = value.getString(Main.ID_PARAM_NAME, "");
        if (Strings.isNullOrEmpty(idempotencyId) || idempotenceState.contains(idempotencyId)) {
            log.info("delay-queue: duplicate event ignore: id={}", idempotencyId);
            return;
        } else {
            idempotenceState.put(idempotencyId, "");
        }

        // See if this is a deleted state then remove the state - we will no longer send this data in retry
        String requestType = value.getString("request_type", "");
        if (Objects.equals(requestType, "delete")) {
            dataState.remove(idempotencyId);
            idempotenceState.remove(idempotencyId);
            return;
        }

        // Store this to state - we will send this when time expire
        StringObjectMap stateData = StringObjectMap.of(
                "payload", value.getStringObjectMap("payload"),
                "idempotency_id", idempotencyId
        );
        if (value.containsKey("ignore_after") && value.get("ignore_after") instanceof Number) {
            stateData.put("ignore_after", value.get("ignore_after"));
        }
        dataState.put(idempotencyId, stateData);

        // Setup retry timers
        long delayInMs = value.getInt("delay_sec", 60) * 1000;
        int retryCount = value.getInt("retry_count", 3);

        for (int i = 1; i <= retryCount; i++) {
            long timer = ctx.timerService().currentProcessingTime() + (i * delayInMs);
            ctx.timerService().registerProcessingTimeTimer(timer);

            if (debugPrintRegisterTime) {
                log.info("delay-queue: timer registered: id={}, time={}", idempotencyId, new Date(timer));
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.OnTimerContext ctx, Collector<StringObjectMap> out) throws Exception {
        StringObjectMap toSend = dataState.get(ctx.getCurrentKey());
        if (toSend != null) {

            Number ignoreAfter = toSend.get("ignore_after", Number.class);
            if (ignoreAfter != null && ignoreAfter.longValue() > 0) {
                DateTime ignoreAfterTime = new DateTime(new Date(ignoreAfter.longValue()));
                if (DateTime.now().isAfter(ignoreAfterTime)) {
                    if (debugPrintOnTimer) {
                        log.info("delay-queue: on timer called: ignore it is expired : id={} data={}, expiryTime={}", ctx.getCurrentKey(), JsonUtils.asJson(toSend), ignoreAfterTime);
                    }
                    return;
                }
            }

            out.collect(toSend);
            if (debugPrintOnTimer) {
                log.info("delay-queue: on timer called: id={} data={}", ctx.getCurrentKey(), JsonUtils.asJson(toSend));
            }
        }
    }
}
