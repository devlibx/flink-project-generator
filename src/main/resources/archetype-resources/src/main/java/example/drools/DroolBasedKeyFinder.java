#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.rule.drools.DroolsHelper;
import io.github.devlibx.easy.rule.drools.ResultMap;
import org.kie.api.runtime.KieSession;

import java.io.Serializable;

public class DroolBasedKeyFinder implements KafkaSourceHelper.ObjectToKeyConvertor<StringObjectMap>, Serializable {
    private final IRuleEngineProvider ruleEngineProvider;

    public DroolBasedKeyFinder(IRuleEngineProvider ruleEngineProvider) {
        this.ruleEngineProvider = ruleEngineProvider;
    }

    private String keyAsString(StringObjectMap value) {
        DroolsHelper droolsHelper = ruleEngineProvider.getDroolsHelper();
        KieSession kSession = droolsHelper.getKieSessionWithAgenda("expiry-event-trigger-partition-key");
        ResultMap result = new ResultMap();
        kSession.insert(result);
        kSession.insert(value);
        kSession.fireAllRules();
        return result.getString("partition-key", "");
    }

    @Override
    public byte[] key(StringObjectMap value) {
        return keyAsString(value).getBytes();
    }

    @Override
    public byte[] getKey(StringObjectMap value) {
        return keyAsString(value).getBytes();
    }

    @Override
    public int partition(StringObjectMap value, byte[] bytes, byte[] bytes1, String s, int[] partitions) {
        String key = keyAsString(value);
        return key.hashCode() % partitions.length;
    }
}
