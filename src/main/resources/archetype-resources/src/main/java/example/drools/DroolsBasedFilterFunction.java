#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.rule.drools.DroolsHelper;
import io.github.devlibx.easy.rule.drools.ResultMap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.kie.api.runtime.KieSession;

public class DroolsBasedFilterFunction implements FilterFunction<StringObjectMap>, KeySelector<StringObjectMap, String> {
    private final IRuleEngineProvider ruleEngineProvider;

    public DroolsBasedFilterFunction(IRuleEngineProvider ruleEngineProvider) {
        this.ruleEngineProvider = ruleEngineProvider;
    }

    @Override
    public boolean filter(StringObjectMap value) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        DroolsHelper droolsHelper = ruleEngineProvider.getDroolsHelper();
        KieSession kSession = droolsHelper.getKieSessionWithAgenda("filter-input-stream");
        ResultMap result = new ResultMap();
        kSession.insert(result);
        kSession.insert(value);
        kSession.fireAllRules();

        // Skip if rule engine skips it
        return !result.getBoolean("skip", false);
    }

    @Override
    public String getKey(StringObjectMap value) throws Exception {

        // Make a new session - we will mark agenda-group to run selected rules
        DroolsHelper droolsHelper = ruleEngineProvider.getDroolsHelper();
        KieSession kSession = droolsHelper.getKieSessionWithAgenda("filter-input-stream");
        ResultMap result = new ResultMap();
        kSession.insert(result);
        kSession.insert(value);
        kSession.fireAllRules();

        return result.getString("group-key");
    }
}
