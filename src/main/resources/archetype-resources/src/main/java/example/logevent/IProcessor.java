#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import ${package}.example.pojo.FlattenLogEvent;

/**
 * Extension - process to enrich log event and flatten log event
 */
public interface IProcessor {
    void process(LogEvent logEvent, FlattenLogEvent flattenLogEvent);
}
