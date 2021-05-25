package io.confluent.ps.router;

import static org.apache.kafka.common.serialization.Serdes.String;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

/**
* Sample for a routing a topic based on the items in the data KStream.
*/
public final class DynamicTopicRouter {
    private final Logger log = LoggerFactory.getLogger(DynamicTopicRouter.class);
    private AdminClient admin;
    private HashMap<String, LocalDateTime> availableDestinationTopics = new HashMap<>();


    /**
    * Dynamic Topic Router Constructor.
    */
    private DynamicTopicRouter() {
    }

    private Boolean topicExists(String topic) {
        // Code and inspiration from:
        // https://github.com/gwenshap/kafka-examples/blob/master/AdminClientExample/src/main/java/org/example/AdminClientExample.java#L71-L103
        Boolean exists = true;

        if ( !availableDestinationTopics.containsKey(topic) ) {
            TopicDescription topicDescription;
            List<String> topicList = Collections.singletonList(topic);
            DescribeTopicsResult demoTopic = admin.describeTopics(topicList);

            try {
                topicDescription = demoTopic.values().get(topic).get();
                log.trace("Description of demo topic: {}", topicDescription);

                // Record that we found this topic
                availableDestinationTopics.put(topic, LocalDateTime.now());
            } catch (ExecutionException e) {
                log.trace("I am at ExecutionException");
                // exit early for almost all exceptions
                if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    log.info("Topic not found: {}", topic);
                    exists = false;
                } else {
                    log.info("Topic not found: {} reason {}", topic, e.getCause());
                    exists = false;
                }
                log.trace("Done with ExecutionException");
            } catch (InterruptedException e) {
                log.trace("I am at InterruptedException");
                log.trace(Arrays.toString(e.getStackTrace()));
                log.trace("Done with InterruptedException");
            }
        } else {
            log.info("AvailableDestinationTopics: {}", availableDestinationTopics.keySet().toString());
        }

        log.trace("Done with topicExists: {}", exists);
        return exists;
    }

    /**
    * Setup the Streams Processors we will be using from the passed in configuration.properties.
    * @param envProps Environment Properties file
    * @return Properties Object ready for KafkaStreams Topology Builder
    */
    protected Properties buildStreamsProperties(Properties envProps) {
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, envProps.getProperty("application.id"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getProperty("bootstrap.servers"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, envProps.getProperty("security.protocol"));
        props.put(SaslConfigs.SASL_MECHANISM, envProps.getProperty("sasl.mechanism"));
        props.put(SaslConfigs.SASL_JAAS_CONFIG, envProps.getProperty("sasl.jaas.config"));

        log.debug("SASL Config------");
        log.debug("bootstrap.servers={}", envProps.getProperty("bootstrap.servers"));
        log.debug("security.protocol={}", envProps.getProperty("security.protocol"));
        log.debug("sasl.mechanism={}",    envProps.getProperty("sasl.mechanism"));
        log.debug("sasl.jaas.config={}",  envProps.getProperty("sasl.jaas.config"));
        log.debug("-----------------");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put("schema.registry.url", envProps.getProperty("schema.registry.url"));


        // Broken negative timestamp
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        props.put(StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return props;
    }

    /**
    * Build the topology from the loaded configuration
    * @param Properties built by the buildStreamsProperties
    * @return The build topology
    */
    protected Topology buildTopology(Properties envProps) {
        log.debug("Starting buildTopology");
        final String inputTopicName      = envProps.getProperty("input.topic.name");
        final String outputTopicPrefix   = envProps.getProperty("output.topic.prefix");
        final String routeByJsonPath     = envProps.getProperty("route.by.jsonpath");
        final String defaultValue        = envProps.getProperty("output.topic.defaultValue");

        final Pattern inputTopicPattern  =  Pattern.compile(inputTopicName);
        log.info("JSON PATH Settings: {}", routeByJsonPath);
        log.info("Default Value: {}", defaultValue);

        final StreamsBuilder builder = new StreamsBuilder();

        // topic contains JSON string data
        final KStream<String, String> jsonStream =
            builder.stream(inputTopicPattern, Consumed.with(Serdes.String(), Serdes.String()));

        jsonStream.to(
            (key, value, recordContext) ->  {
                String customer = defaultValue;
                //log.info("This messages recordContext: {}", recordContext);
                try {
                    customer = JsonPath.parse(value).read(routeByJsonPath, String.class);
                } catch(PathNotFoundException e) {
                    log.info("JSONPath[{}] did not find a value in this message: {}", routeByJsonPath, value);
                }

                String toTopic = outputTopicPrefix + "." + customer;
                String defaultTopic = outputTopicPrefix + "." + defaultValue;

                if (topicExists(toTopic))
                    return outputTopicPrefix + "." + customer;
                else
                    return defaultTopic;

            },
            Produced.with(Serdes.String(), Serdes.String())
        );
        return builder.build();
    }


    /**
    * Load in the Environment Properties that were passed in from the CLI.
    * @param fileName
    * @return
    * @throws IOException
    */
    protected Properties loadEnvProperties(String fileName) throws IOException {
        Properties envProps = new Properties();

        try (
        FileInputStream input = new FileInputStream(fileName);
        ) {
            envProps.load(input);
        }
        return envProps;
    }

    /**
    * Main function that handles the life cycle of the Kafka Streams app.
    * @param configPath
    * @throws IOException
    */
    private void run(String configPath) throws IOException {

        Properties envProps = this.loadEnvProperties(configPath);
        Properties streamProps = this.buildStreamsProperties(envProps);

        // initialize admin client
        Properties adminProps = (Properties) streamProps.clone();
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000);
        adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 1000);
        admin = AdminClient.create(adminProps);

        Topology topology = this.buildTopology(envProps);

        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        } finally {
            log.info("Closing our Admin Topic Helper");
            admin.close(Duration.ofSeconds(5));
        }
        System.exit(0);
    }

    private static void exampleProperties() {
        System.out.println("Please create a configuration properties file and pass it on the command line as an argument");
        System.out.println("Sample env.properties:");
        System.out.println("----------------------------------------------------------------");
        System.out.println("application.id=enrich-some-json");
        System.out.println("bootstrap.servers=big-host-1.datadisorder.dev:9092");
        System.out.println("schema.registry.url=http://big-host-1.datadisorder.dev:8081");
        System.out.println("input.topic.name=bro_sample");
        System.out.println("output.topic.name=bro_enriched");
        System.out.println("error.topic.name=bro_error");
        System.out.println("security.protocol=PLAINTEXT");
        System.out.println("sasl.mechanism=PLAIN");
        System.out.println("sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";");
        System.out.println("jsonpath.to.vlan.id=$['id.vlan']");
        System.out.println("jsonpath.for.customer=SomeLongerValue");
        System.out.println("confluent.metrics.reporter.bootstrap.servers=big-host-1.datadisorder.dev:9092");
        System.out.println("confluent.metrics.reporter.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");
        System.out.println("confluent.metrics.reporter.sasl.mechanism=PLAIN");
        System.out.println("confluent.metrics.reporter.security.protocol=PLAINTEXT");
        System.out.println("----------------------------------------------------------------");
    }

    /**
    *  Run this with an arg for the properties file
    * @param args
    * @throws IOException
    */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            exampleProperties();
            throw new IllegalArgumentException("This program takes one argument: the path to an environment configuration file.");
        }

        new DynamicTopicRouter().run(args[0]);
    }
}
