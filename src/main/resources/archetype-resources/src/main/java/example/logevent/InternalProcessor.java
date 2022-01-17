#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import ${package}.example.pojo.FlattenLogEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class InternalProcessor extends KeyedProcessFunction<String, LogEvent, FlattenLogEvent> {
    private final List<IProcessor> processors;

    public InternalProcessor() {
        this.processors = new ArrayList<>();
    }

    @Override
    public void processElement(LogEvent value, Context ctx, Collector<FlattenLogEvent> out) throws Exception {
        try {
            FlattenLogEvent flattenLogEvent = FlattenLogEvent.convert(value);
            processors.forEach(processor -> {
                processor.process(value, flattenLogEvent);
            });
            out.collect(flattenLogEvent);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
