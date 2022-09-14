#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.job;

import io.gitbub.devlibx.easy.helper.map.StringObjectMap;
import io.github.devlibx.easy.flink.utils.v2.config.Configuration;
import io.github.devlibx.easy.rule.drools.ResultMap;
import io.github.devlibx.miscellaneous.flink.common.KeyPair;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

@SuppressWarnings("ALL")
public class DummyDeleteMe {

    // Input Stream is read from Kafka - it contains all events coming in Kafka topic
    void run(StreamExecutionEnvironment env, Configuration configuration, DataStream<StringObjectMap> inputStream, Class<Configuration> cls) {

        // This is the flow we will build here
        // Read from Kafka
        //   => Filter events (e.g. only process where status=COMPLETED)
        //       => Give some key for flink partition (e.g. user id is good enough - or any other id like order id)
        //           => Process (aggregation work)
        //               => Save to DB and out to Kafka

        // First of all read following code on the aggregation -> this will make it easy for you to understand code
        // https://github.com/devlibx/java-miscellaneous/blob/master/java-util/src/main/java/io/github/devlibx/miscellaneous/util/aggregation/Demo.java

        // To run your business logic we call the rule engine
        // This example is using following rune file
        // Read file - src/main/resources/test_aggregate_sample_rule.drl
        //     Each rule fills a var called resultMap with the business logic

        /* A example event
          {
           "data": {
                    "order_status": "COMPLETED",
                    "merchant_id": "Apple",
                    "category": "phone",
                    "order_id": "order_1"
                },
                "user_id": "1234",
                "timestamp": 1662406736235
            }

            We want to do 2 aggregation:

            1. user_id: how many time user got completed event
               -> in this example we save it with key user_id${symbol_pound}na = <counter>
                       e.g. 1234:na = 1... with more events 2, ...

            2. user_id:category: what all category user ordered with completed event
               -> in this example we save it with key user_id${symbol_pound}<category> = "cat1,cat2"
                     e.g. 1234${symbol_pound}phone = "Apple"... with more events "Apple,Samsumg", ....
         */


        // Step 1 - We have to filter with business logic
        SingleOutputStreamOperator<StringObjectMap> filteredRecords = inputStream.filter(value -> {

            ResultMap resultMap = null;
            // This will be filled by calling rule engine with [agenda-group "filter-input-stream"]
            // It wills 2 var
            //  skip        = true/false
            //  group-key   = a id which we can use as a key e.g. may be user id, or order id
            //                [not used in filter - we will use it below]

            if (resultMap.getBoolean("skip", false /* default is false */)) {
                return false;
            } else {
                return true;
            }
        });

        // Step 2 - Key all the results so it goes to correct processor (internal to Flink - dont wrorry about this)
        KeyedStream<StringObjectMap, String> keyedData = filteredRecords.keyBy(new KeySelector<StringObjectMap, String>() {
            @Override
            public String getKey(StringObjectMap value) throws Exception {

                // As explained above we will call with rule engine with [agenda-group "filter-input-stream"] again
                // This time we will use "group-key"
                ResultMap resultMap = null;

                return resultMap.getString("group-key");
            }
        });

        // Step 3 - Process the data
        keyedData.process(new KeyedProcessFunction<String, StringObjectMap, Object>() {
            @Override
            public void processElement(StringObjectMap value, KeyedProcessFunction<String, StringObjectMap, Object>.Context ctx, Collector<Object> out) throws Exception {
                ResultMap resultMap = null;

                // -----------------------------------------------------------------------------------------------------
                // Step 1 - Find the keys to check if we already have this user info in flink state

                // First we need to find the keys persist in state - calling [agenda-group "initial-event-trigger-get-state-to-fetch"]
                List<KeyPair> keys = resultMap.getList("states-to-provide", KeyPair.class);
                // Here the rule will return following
                //      Arrays.asList(new KeyPair("1234", "na"), new KeyPair("1234", "phone")
                // -> -> you don't need to do anything -> this framework will get it and pass it to you

                // If yes, then pass to the next flow, other pass empty aggregation so it can start from zero

                // -----------------------------------------------------------------------------------------------------
                // Step 2 - Aggregation
                // We call [agenda-group "initial-event-trigger"]
                //
                // Again you don't have to do anything -> just return data from rule. Framework will handle it


                // This is the logic in the initial-event-trigger -> this is the data retruned. You have to follow
                //                                                   the same thing
                //
                //        Map<KeyPair, StringObjectMap> forwardObjects = new HashMap<>();
                //        forwardObjects.put(primaryIdKeyPair, StringObjectMap.of(
                //                "key_pair", primaryIdKeyPair,
                //                "aggregation", aggregationOrdersByUser
                //        ));
                //        forwardObjects.put(primaryAndSecondaryIdKeyPair, StringObjectMap.of(
                //                "key_pair", primaryAndSecondaryIdKeyPair,
                //                "aggregation", aggregationOfMerchantsUserUsed
                //        ));
                //
                //        Map<KeyPair, StringObjectMap> retainObjects = new HashMap<>();
                //        retainObjects.put(primaryIdKeyPair, StringObjectMap.of("aggregation", aggregationOrdersByUser));
                //        retainObjects.put(primaryAndSecondaryIdKeyPair, StringObjectMap.of("aggregation", aggregationOfMerchantsUserUsed));
                //
                //        resultMap.put("retain-state", true);
                //        resultMap.put("retain-objects", retainObjects);
                //        resultMap.put("forward-objects", forwardObjects);
                //
                //        You will notice retain object and forward object are same -> because here we keep the
                //        same object in state and also forward to output


                // This will do the aggregation and will return 2 thing
                // First (retain-objects) - state to save in flink (for next time)
                //          1234:na = 1
                //          1234${symbol_pound}phone = "Apple"
                // -> you don't need to do anything -> this framework will store it
                //          Now next time process is called, this same function will not get empty but the previosu state
                //          and you will udpate the counter to "2" and "Apple,Samsumg


                // Second - We may want to store this data to DB or to some Kafka
                // (forward-objects) will be send to out -> This framework already saves it in DDB
            }
        });

    }
}
