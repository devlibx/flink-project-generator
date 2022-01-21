#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.github.devlibx.flink.example.pojo.FlattenLogEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class InternalProcessor extends KeyedProcessFunction<String, LogEvent, FlattenLogEvent> {
    private final List<IProcessor> processors;
    private transient MapState<String, FlattenLogEvent> logEventMapState;

    public InternalProcessor() {
        this.processors = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) {
        MapStateDescriptor<String, FlattenLogEvent> mapStateDescriptor = new MapStateDescriptor<>(
                "log-events",
                String.class,
                FlattenLogEvent.class
        );
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(35))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        mapStateDescriptor.enableTimeToLive(ttlConfig);
        logEventMapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(LogEvent value, Context ctx, Collector<FlattenLogEvent> out) throws Exception {
        try {

            // Build flatten event from log event
            FlattenLogEvent toPersist = FlattenLogEvent.convert(value);

            // Merge with existing object
            try {
                if (logEventMapState.contains(toPersist.key())) {
                    FlattenLogEvent existingEventInState = logEventMapState.get(toPersist.key());
                    toPersist = FlattenLogEvent.merge(existingEventInState, toPersist);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Run all processors to update
            for (IProcessor processor : processors) {
                // Process event
                processor.process(value, toPersist);
            }

            // Persist to map state
            try {
                logEventMapState.put(toPersist.key(), toPersist);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Send the event out
            out.collect(toPersist);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
