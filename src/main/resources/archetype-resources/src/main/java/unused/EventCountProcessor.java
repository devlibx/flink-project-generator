#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.unused;

import ${package}.pojo.Order;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.Collection;

@Slf4j
public class EventCountProcessor extends ProcessWindowFunction<Order, Alert, String, TimeWindow> {


    @Override
    public void process(String key, Context context, Iterable<Order> elements, Collector<Alert> out) throws Exception {
        int count = 0;
        if (elements instanceof Collection) {
            count = ((Collection<?>) elements).size();
        } else {
            for (Object i : elements) {
                count++;
            }
        }

        if (count > 2) {
            Alert alert = new Alert();
            alert.setId(count);
            out.collect(alert);
        }
    }
}
