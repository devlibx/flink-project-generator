#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.example.pojo;


import io.gitbub.devlibx.easy.helper.common.LogEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FlattenLogEventTest {

    @Test
    public void testFlatten() {
        LogEvent.setGlobalServiceName("test");

        LogEvent _first = LogEvent.build("sent")
                .data("udf_long_1", 11, "udf_long_2", 20)
                .build();
        FlattenLogEvent first = FlattenLogEvent.convert(_first);

        LogEvent _second = LogEvent.build("sent")
                .data("udf_long_1", 12)
                .build();
        FlattenLogEvent second = FlattenLogEvent.convert(_second);

        FlattenLogEvent fe = FlattenLogEvent.merge(first, second);
        Assertions.assertEquals(12, fe.getUdfLong1());
        Assertions.assertEquals(20, fe.getUdfLong2());
    }
}