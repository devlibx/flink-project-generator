#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.rule.drools.ResultMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;

public class DroolsBasedFilterFunction implements FilterFunction<StringObjectMap>, KeySelector<StringObjectMap, String> {
    private final IRuleEngineProvider ruleEngineProvider;

    public DroolsBasedFilterFunction(IRuleEngineProvider ruleEngineProvider) {
        this.ruleEngineProvider = ruleEngineProvider;
    }

    @Override
    public boolean filter(StringObjectMap value) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        ResultMap result = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result);

        // Skip if rule engine skips it
        return !result.getBoolean("skip", false);
    }

    @Override
    public String getKey(StringObjectMap value) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules


        ResultMap result = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("filter-input-stream", value, result);


        return result.getString("group-key");
    }
}
