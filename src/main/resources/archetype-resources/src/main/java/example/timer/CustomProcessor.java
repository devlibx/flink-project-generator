#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.timer;

import ${package}.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

@Slf4j
public class CustomProcessor extends KeyedProcessFunction<Long, Order, Alert> {
    private static final long serialVersionUID = 1L;

    private transient ValueState<Long> timerState;
    private transient MapState<Long, Order> mapState;

    @Override
    public void open(Configuration parameters) {

        // This is to keep the time when we setup a timer - may not be needed.
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);

        // This is imp - we will keep last order in this map for a customer
        MapStateDescriptor<Long, Order> mapStateDescriptor = new MapStateDescriptor<>(
                "map",
                Long.class,
                Order.class
        );
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public void processElement(Order value, Context context, Collector<Alert> out) throws Exception {

        // Setup a timer for 1 sec - we will get called after 1 sec
        long timer = context.timerService().currentProcessingTime() + 1000;
        context.timerService().registerProcessingTimeTimer(timer);

        // Store the order objet is our state to be accessed when timer is called
        timerState.update(context.timerService().currentProcessingTime());
        mapState.put(value.getCustomerKey(), value);
    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
        // What os ctx.getCurrentKey()
        // This is the key which is used in partition -> in this code we used customer key as our context key
        // So we will get the customer key from context.
        try {
            log.info("Timer called for customerId={}", ctx.getCurrentKey());
            Order order = mapState.get(ctx.getCurrentKey());
            log.info("Timer called for customerId={} and got object from mapStore={}", ctx.getCurrentKey(), order);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Just out a alert - not mandatory
        Alert alert = new Alert();
        // out.collect(alert);

        // Just clear the timer value
        timerState.clear();
    }
}
