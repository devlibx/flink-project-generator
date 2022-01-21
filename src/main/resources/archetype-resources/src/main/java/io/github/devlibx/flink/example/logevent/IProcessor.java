#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.logevent;

import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.github.devlibx.flink.example.pojo.FlattenLogEvent;

/**
 * Extension - process to enrich log event and flatten log event
 */
public interface IProcessor {
    void process(LogEvent logEvent, FlattenLogEvent flattenLogEvent);
}
