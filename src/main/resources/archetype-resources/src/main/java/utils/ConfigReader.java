#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.utils;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class ConfigReader {

    public static ParameterTool readConfigsFromS3(String bucket, String file, boolean failOnError) throws IOException {
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
            S3Object object = s3Client.getObject(new GetObjectRequest(bucket, file));
            InputStream objectData = object.getObjectContent();
            ParameterTool parameter = ParameterTool.fromPropertiesFile(objectData);
            objectData.close();
            return parameter;
        } catch (IOException e) {
            if (failOnError) {
                throw e;
            } else {
                return ParameterTool.fromSystemProperties();
            }
        }
    }
}
