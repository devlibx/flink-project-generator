#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.common;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.github.devlibx.easy.messaging.config.MessagingConfigs;
import lombok.Data;


@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaMessagingTestConfig {
    public MessagingConfigs messaging;
}
