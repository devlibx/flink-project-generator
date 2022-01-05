#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args, RunJob runJob) throws Exception {

        // Read config from S3
        ParameterTool argsParams = ParameterTool.fromArgs(args);
        String bucket = argsParams.getRequired("bucket");
        String filePath = argsParams.getRequired("file");
        ParameterTool parameter = ConfigReader.readConfigsFromS3(bucket, filePath, false);

        // Run the input job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        runJob.run(env, parameter);
        env.execute("Fraud Detection");
    }

    public interface RunJob {
        void run(StreamExecutionEnvironment env, ParameterTool parameter);
    }
}
