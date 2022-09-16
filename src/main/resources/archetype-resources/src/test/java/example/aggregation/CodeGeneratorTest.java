#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.aggregation;

import io.github.devlibx.miscellaneous.flink.drools.generator.DroolFileGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.io.IOException;

public class CodeGeneratorTest {

    @Test
    @EnabledOnOs(OS.MAC)
    public void generateDrlFile() throws IOException {
        String runPath = new File(".").getAbsoluteFile().getPath();

        String inputFile = runPath + "/src/test/java/${packageInPathFormat}/example/aggregation/TestDroolsLogic.java";
        String outputFile = runPath + "/src/main/resources/test_aggregate_sample_rule.drl";

        DroolFileGenerator fileGenerator = new DroolFileGenerator(inputFile);
        fileGenerator.generateOut(outputFile);

    }
}
