#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package io.github.devlibx.flink.common;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestConfigUtil {
    public static String cloneConfig(String path, String uniqueId) throws IOException {
        String content = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        content = content.replace("EventCountJob.input.groupId=", "EventCountJob.input.groupId=" + uniqueId);
        FileUtils.writeStringToFile(new File("/tmp/flink_test_" + uniqueId), content, StandardCharsets.UTF_8);
        return "/tmp/flink_test_" + uniqueId;
    }
}
