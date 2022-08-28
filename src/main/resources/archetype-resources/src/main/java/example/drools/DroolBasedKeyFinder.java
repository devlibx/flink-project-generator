#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.KafkaSourceHelper;
import io.github.devlibx.easy.rule.drools.ResultMap;

import java.io.Serializable;

public class DroolBasedKeyFinder implements KafkaSourceHelper.ObjectToKeyConvertor<StringObjectMap>, Serializable {
    private final IRuleEngineProvider ruleEngineProvider;

    public DroolBasedKeyFinder(IRuleEngineProvider ruleEngineProvider) {
        this.ruleEngineProvider = ruleEngineProvider;
    }

    private String keyAsString(StringObjectMap value) {
        ResultMap result = new ResultMap();
        ruleEngineProvider.getDroolsHelper().execute("expiry-event-trigger-partition-key", value, result);
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
