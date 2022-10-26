#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.sink.http;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.gitbub.devlibx.easy.helper.ApplicationContext;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.gitbub.devlibx.easy.helper.metrics.IMetrics;
import io.gitbub.devlibx.easy.helper.string.StringHelper;
import io.github.devlibx.easy.flink.utils.ConfigReader;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.http.module.EasyHttpModule;
import io.github.devlibx.easy.http.util.EasyHttp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class HttpSinkOperatorTest {
    private Configuration configuration;

    @BeforeEach
    public void setup() throws IOException {
        String path = new File(".").getAbsolutePath() + "/src/test/resources/test-http-sink-config-v2.yaml";
        String configString = ConfigReader.readConfigsFromFileAsString(path, true);
        Yaml yaml = new Yaml();
        Map obj = yaml.load(configString);
        configuration = JsonUtils.getCamelCase().readObject((new StringHelper()).stringify(obj), Configuration.class);
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(IMetrics.class).to(IMetrics.ConsoleOutputMetrics.class).in(Scopes.SINGLETON);
            }
        }, new EasyHttpModule());
        ApplicationContext.setInjector(injector);
        EasyHttp.setup(configuration.getHttpConfig());
    }

    @Test
    @EnabledOnOs(OS.MAC)
    public void testProcessEvent() {
        // This is how my input looks like {"data": {"post_id": "1"}}
        StringObjectMap data = StringObjectMap.of(
                "data", StringObjectMap.of(
                        "post_id", "1"
                )
        );

        // Make the call and see if your API is called
        HttpSinkOperator httpSinkOperator = new HttpSinkOperator(configuration);
        httpSinkOperator.processEvent(data, false);
    }
}