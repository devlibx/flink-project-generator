#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.window.eventcount;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.functions.EventCountAggregatorInWindowKeyedProcessFunction;
import io.github.devlibx.easy.flink.functions.EventNotReceivedInWindowKeyedProcessFunction;
import io.github.devlibx.easy.flink.functions.common.EventCount;
import io.github.devlibx.easy.flink.window.CountAggregatorFunction;
import ${package}.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

@Slf4j
public class Pipeline {

    SingleOutputStreamOperator<EventCount> process(StreamExecutionEnvironment env, DataStream<Order> orders, StringObjectMap parameter) {

        // FOR CLIENT TO CHANGE - Filter orders
        // Let's say you want to filter only failed orders.
        DataStream<Order> highPriorityOrders = orders
                .filter(order -> "F".equals(order.getOrderStatus()));

        // Get the window size and slide from config
        int size = parameter.getInt("windowDuration", 60);
        int slide = parameter.getInt("emitResultEventNthSeconds", 5);

        // Collect items for last 60(n) sec, and emit this data every 5(n) sec
        return highPriorityOrders

                // Order by status - you can ignore it and use WindowAll function if required
                .keyBy(Order::getOrderStatus)

                // Collect window in a sliding window
                .window(SlidingEventTimeWindows.of(Time.seconds(size), Time.seconds(slide)))

                // Sum all the orders in this window
                .aggregate(new CountAggregatorFunction<>())

                .keyBy(value -> "1")

                // Finally output the result at the end of each slide
                // .process(new EventNotReceivedInWindowKeyedProcessFunction(slide + 10))
                .process(new EventCountAggregatorInWindowKeyedProcessFunction())

                // Name this operation for display
                .name("OrderAggregator");
    }
}


