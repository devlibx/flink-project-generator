#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DebugSync<IN> extends RichSinkFunction<IN> {
    @Override
    public void invoke(IN value, Context context) throws Exception {
        System.out.println("DebugSync: " + JsonUtils.asJson(value));
    }
}
