#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.sink.http;

import com.google.common.base.Strings;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.gitbub.devlibx.easy.helper.ApplicationContext;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.gitbub.devlibx.easy.helper.metrics.IMetrics;
import io.github.devlibx.easy.http.module.EasyHttpModule;
import io.github.devlibx.easy.http.util.Call;
import io.github.devlibx.easy.http.util.EasyHttp;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Date;

@Slf4j
public class HttpSinkProcessor extends KeyedProcessFunction<String, StringObjectMap, StringObjectMap> {
    private final io.github.devlibx.easy.flink.utils.v2.config.Configuration configuration;
    private final boolean debugPrintOnReceivedEvent;
    private final boolean debugPrintOnTimer;
    private transient MapState<String, StringObjectMap> dataState;
    private transient MapState<String, String> idempotenceState;
    private final int delaySec;
    private final int retryCount;
    private final String path;

    public HttpSinkProcessor(io.github.devlibx.easy.flink.utils.v2.config.Configuration configuration) {
        this.configuration = configuration;
        debugPrintOnReceivedEvent = configuration.getMiscellaneousProperties().getBoolean("  debug-print-on-received-event", false);
        debugPrintOnTimer = configuration.getMiscellaneousProperties().getBoolean("delay-trigger-debug-on-timer", false);
        delaySec = configuration.getMiscellaneousProperties().getInt("delay-sec", 30);
        retryCount = configuration.getMiscellaneousProperties().getInt("retry-count", 1);
        path = configuration.getMiscellaneousProperties().getString("key-path", "TODO-Key-Path");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // Setup HTTP
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(IMetrics.class).to(IMetrics.ConsoleOutputMetrics.class).in(Scopes.SINGLETON);
            }
        }, new EasyHttpModule());
        ApplicationContext.setInjector(injector);
        EasyHttp.setup(this.configuration.getHttpConfig());

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
        String idempotencyId = ctx.getCurrentKey();
        if (Strings.isNullOrEmpty(idempotencyId) || idempotenceState.contains(idempotencyId)) {
            log.info("delay-queue: duplicate event ignore: id={}", idempotencyId);
            return;
        } else {
            idempotenceState.put(idempotencyId, "");
        }

        // Make the API call and set retry if it failed
        try {
            if (debugPrintOnReceivedEvent) log.info("Event received make http call: data={}", value);
            processHttpCall(value);
            if (debugPrintOnReceivedEvent) log.info("Event received make http call: data={}", value);
        } catch (Exception e) {
            log.error("Failed to make HTTP call, setting for retry: data={}", value);
            processElementForRetry(value, idempotencyId, ctx);
        }
    }

    private void processElementForRetry(StringObjectMap value, String idempotencyId, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.Context ctx) throws Exception {

        // Store this to state - we will send this when time expire
        StringObjectMap stateData = StringObjectMap.of(
                "payload", value,
                "idempotency_id", idempotencyId
        );
        dataState.put(idempotencyId, stateData);

        // Setup retry timers
        long delayInMs = delaySec * 1000L;
        for (int i = 1; i <= retryCount; i++) {
            long timer = ctx.timerService().currentProcessingTime() + (i * delayInMs);
            ctx.timerService().registerProcessingTimeTimer(timer);
            if (debugPrintOnReceivedEvent) {
                log.info("delay-queue: timer registered: id={}, time={}", idempotencyId, new Date(timer));
            }
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, StringObjectMap, StringObjectMap>.OnTimerContext ctx, Collector<StringObjectMap> out) throws Exception {
        StringObjectMap toSend = dataState.get(ctx.getCurrentKey());
        if (toSend != null) {
            try {
                if (debugPrintOnTimer) log.info("OnTimer called for retry: data={}", toSend);
                processHttpCall(toSend);
                if (debugPrintOnTimer) log.info("[Success] OnTimer called for retry: data={}", toSend);
            } catch (Exception e) {
                log.error("Failed to make HTTP call in retry: data={}", toSend);
            }
        }
    }

    private void processHttpCall(StringObjectMap value) {
        String postId = value.path(path, String.class);
        StringObjectMap result = EasyHttp.callSync(Call.builder(StringObjectMap.class)
                .withServerAndApi("jsonplaceholder", "getPosts")
                .addPathParam("id", postId)
                .build());
        System.out.println(JsonUtils.asJson(result));
    }
}
