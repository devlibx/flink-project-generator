#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.example.logevent;


import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.gitbub.devlibx.easy.helper.common.LogEvent;
import io.gitbub.devlibx.easy.helper.json.JsonUtils;
import io.gitbub.devlibx.easy.helper.yaml.YamlUtils;
import io.github.devlibx.easy.messaging.config.MessagingConfigs;
import io.github.devlibx.easy.messaging.consumer.IConsumer;
import io.github.devlibx.easy.messaging.kafka.module.MessagingKafkaModule;
import io.github.devlibx.easy.messaging.module.MessagingModule;
import io.github.devlibx.easy.messaging.producer.IProducer;
import io.github.devlibx.easy.messaging.service.IMessagingFactory;
import ${package}.common.KafkaMessagingTestConfig;
import ${package}.example.pojo.FlattenLogEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@SuppressWarnings({"ResultOfMethodCallIgnored", "OptionalGetWithoutIsPresent"})
public class KafkaInputLogEventAndOutputFlattenEventTest {
    private static IProducer producer;
    private static IConsumer consumer;

    @BeforeAll
    public static void setup() {
        // Read config from file
        KafkaMessagingTestConfig kafkaConfig = YamlUtils.readYaml("messaging.yaml", KafkaMessagingTestConfig.class);
        kafkaConfig.messaging.getConsumers().get("orders").put("group.id", UUID.randomUUID().toString());

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(MessagingConfigs.class).toInstance(kafkaConfig.messaging);
            }
        }, new MessagingKafkaModule(), new MessagingModule());

        // Get messaging factory and initialize the factory
        IMessagingFactory messagingFactory = injector.getInstance(IMessagingFactory.class);
        messagingFactory.initialize();

        producer = messagingFactory.getProducer("orders").get();
        consumer = messagingFactory.getConsumer("orders").get();
    }

    @Test
    @Disabled
    public void testEndToEnd() throws InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String uid = UUID.randomUUID().toString();
        long time = System.currentTimeMillis();
        LogEvent.setGlobalServiceName("test");
        LogEvent event = LogEvent.build("send")
                .entity("user", uid)
                .data("name", "harish", "timestamp", time)
                .build();
        DataStream<LogEvent> inputStream = env.fromElements(event);

        ParameterTool parameterTool = Mockito.mock(ParameterTool.class);
        Mockito.when(parameterTool.getRequired("LogEventJob.output.brokers")).thenReturn("localhost:9092");
        Mockito.when(parameterTool.getRequired("LogEventJob.output.topic")).thenReturn("orders_out");


        // Execute pipeline
        Thread mainThread = new Thread(() -> {
            LogEventToFlattenLogEventJob job = new LogEventToFlattenLogEventJob();
            job.internalRun(env, parameterTool, inputStream);
            try {
                env.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        mainThread.start();

        // Listen for output -> this will tell if we are good or not
        CountDownLatch cl = new CountDownLatch(1);
        AtomicReference<FlattenLogEvent> fe = new AtomicReference<>();
        new Thread(() -> {
            consumer.start((data, noop) -> {
                try {
                    FlattenLogEvent ev = JsonUtils.readObject(data.toString(), FlattenLogEvent.class);
                    System.out.println("------>>>> Got event=" + ev);
                    if (uid.equals(ev.getEntityId())) {
                        fe.set(ev);
                        cl.countDown();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }).start();
        cl.await(10, TimeUnit.SECONDS);

        Assertions.assertNotNull(fe.get(), "Expected kafka event in out topic and consumer getting it (make sure Kafka is running and has a topic 'orders_out'");
        Assertions.assertEquals(uid, fe.get().getId());
    }

    @Test
    @Disabled
    public void testPipeline() throws Exception {

        // Start the job
        new Thread(() -> {
            try {
                Path currentRelativePath = Paths.get("");
                String path = currentRelativePath.toAbsolutePath().toString();
                LogEventToFlattenLogEventJob.main(new String[]{"--config", path + "/config.properties"});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // Test UUID
        String uid = UUID.randomUUID().toString();


        // Listen for output -> this will tell if we are good or not
        CountDownLatch cl = new CountDownLatch(1);
        AtomicReference<FlattenLogEvent> fe = new AtomicReference<>();
        new Thread(() -> {
            consumer.start((data, noop) -> {
                try {
                    FlattenLogEvent ev = JsonUtils.readObject(data.toString(), FlattenLogEvent.class);
                    System.out.println("------>>>> Got event=" + ev);
                    if (uid.equals(ev.getEntityId())) {
                        fe.set(ev);
                        cl.countDown();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }).start();

        // Send event and see if the pipeline ran and gave the result
        Thread.sleep(5000);
        LogEvent.setGlobalServiceName("test");
        LogEvent event = LogEvent.build("send")
                .entity("user", uid)
                .data("name", "harish", "timestamp", System.currentTimeMillis())
                .build();
        producer.send(event.getEntity().getId(), JsonUtils.asJson(event).getBytes());
        System.out.println("------>>>> Emit event for id=" + uid);

        cl.await(30, TimeUnit.SECONDS);
        Assertions.assertEquals(uid, fe.get().getId());
    }
}