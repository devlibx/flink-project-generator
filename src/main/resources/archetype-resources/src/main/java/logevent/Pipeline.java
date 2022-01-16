#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import ${package}.pojo.FlattenLogEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Pipeline {

    /**
     * Processor pipeline
     */
    SingleOutputStreamOperator<FlattenLogEvent> process(StreamExecutionEnvironment env, DataStream<LogEvent> events, StringObjectMap parameter) {

        // Collect items for last 60(n) sec, and emit this data every 5(n) sec
        return events
                // Order by status - you can ignore it and use WindowAll function if required
                .keyBy(logEvent -> logEvent.getEntity().getType() + "-" + logEvent.getEntity().getId())
                // Setup event processor
                .process(new InternalProcessor())
                .name("LogEventToFlattenLogEventConvertor");
    }
}
