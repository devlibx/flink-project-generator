#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.sink.http;

import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.http.util.Call;
import io.github.devlibx.easy.http.util.EasyHttp;

import java.io.Serializable;

public class HttpSinkOperator implements Serializable {
    private final String path;

    public HttpSinkOperator(io.github.devlibx.easy.flink.utils.v2.config.Configuration configuration) {
        path = configuration.getMiscellaneousProperties().getString("key-path", "TODO-Key-Path");
    }

    public void processEvent(StringObjectMap value, boolean isRetryCall) {
        String postId = value.path(path, String.class);
        StringObjectMap result = EasyHttp.callSync(Call.builder(StringObjectMap.class)
                .withServerAndApi("jsonplaceholder", "getPosts")
                .addPathParam("id", postId)
                .build());
        System.out.println(JsonUtils.asJson(result));
    }
}