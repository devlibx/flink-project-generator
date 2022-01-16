#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import ${package}.pojo.FlattenLogEvent;

public interface IProcessor {
    void process(LogEvent logEvent, FlattenLogEvent flattenLogEvent);
}
