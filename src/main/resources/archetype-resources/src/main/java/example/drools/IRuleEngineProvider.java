#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.drools;

import io.github.devlibx.easy.rule.drools.DroolsHelper;
import lombok.Getter;

import java.io.Serializable;

public interface IRuleEngineProvider {
    DroolsHelper getDroolsHelper();

    class ProxyDroolsHelper implements IRuleEngineProvider, Serializable {
        private IRuleEngineProvider ruleEngineProvider;
        private final String ruleFile;

        public ProxyDroolsHelper(String ruleFile) {
            this.ruleFile = ruleFile;
        }

        @Override
        public DroolsHelper getDroolsHelper() {
            if (ruleEngineProvider == null) {
                try {
                    ruleEngineProvider = new MainDroolsHelper(ruleFile);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            return ruleEngineProvider.getDroolsHelper();
        }
    }

    class MainDroolsHelper implements IRuleEngineProvider, Serializable {
        @Getter
        private final DroolsHelper droolsHelper;

        MainDroolsHelper(String ruleFile) throws Exception {
            droolsHelper = new DroolsHelper();
            droolsHelper.initialize(ruleFile);
        }
    }
}
