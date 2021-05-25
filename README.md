# Dynamic Topic Router
Route a set of JSON data topics using a JSONPath to a set of topics determined by the contents of the message

### Build and Execution Environment
* Java 8
* Confluent Platform 5.5.x or newer

## Build
Use Maven to build the KStream Application.

```
mvn clean package
```

A successful build will create a target directory with the following two jar files:
* dynamic-topic-router-0.1.jar
* dynamic-topic-router-0.1-jar-with-dependencies.jar

The `dynamic-topic-router-0.1-jar-with-dependencies.jar` file contains all the dependencies needed to run the application. Therefore, this dependencies jar file should be the file executed when running the KStream Application.

## Execution Configuration
The KStream Application requires a configuration properties file.

Example:
```
application.id=dynamic-topic-router
bootstrap.servers=big-host-1.datadisorder.dev:9092
schema.registry.url=http://big-host-1.datadisorder.dev:8081
input.topic.name=data.input.*
output.topic.prefix=data.for.customer
output.topic.defaultValue=notFound

# Sample topics used for testing
data.x.topic.name=data.input.x
data.y.topic.name=data.input.y
data.z.topic.name=data.input.z
customer.a.topic.name=data.for.customer.a
customer.b.topic.name=data.for.customer.b
customer.c.topic.name=data.for.customer.c
customer.defaultValue.topic.name=data.for.customer.notFound


route.by.jsonpath=customer

security.protocol=PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin\";
jsonpath.to.vlan.id=$['id.vlan']
jsonpath.for.customer=SomeLongerValue
confluent.metrics.reporter.bootstrap.servers=big-host-1.datadisorder.dev:9092
confluent.metrics.reporter.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";
confluent.metrics.reporter.sasl.mechanism=PLAIN
confluent.metrics.reporter.security.protocol=PLAINTEXT
```

With the above configuration, the KStreams application will connect to the Kafka Brokers identified by the `bootstrap.servers` cluster making use of the `security.protocol` and `sasl.` configuration. The KStreams Application will use a consumer group with the `application.id` and read its input from topics matching the `input.topic.name` pattern. The messages will be parsed as JSON, the `route.by.jsonpath` will extract a value and append it to the `output.topic.prefix` with a `.`. If the generated topic does not exist, the output topic will be generated from the `output.topic.prefix` and the `outpur.topic.defaultValue`. To horizontally scale the KStream, make sure the `input.topic.name` has multiple partitions and start another jvm with the same configuration properties file.

## Execution
Run the `dynamic-topic-router-0.1-jar-with-dependencies.jar` with Java 8.

```
java -jar dynamic-topic-router-0.1-jar-with-dependencies.jar configuration/dev.properties
```


## Test
Create input/output/error topics

```
PROPERTIES=configuration/dev.properties
for topic in `cat $PROPERTIES | grep "topic.name" | cut -d "=" -f 2`; do \
echo "Create $topic"; \
kafka-topics --bootstrap-server big-host-1.datadisorder.dev:9092 --create --topic $topic --replication-factor 1 --partitions 3 --command-config $PROPERTIES; \
done
```

Assign ACLs if needed
```
TODO
```

Start the Kafka Stream (see Execution Configuration and Execution)

Push the sample messages (added to the background, to make it run "faster")
```
echo '{"customer":"a"}' | jq -rc | kafka-console-producer --bootstrap-server big-host-1.datadisorder.dev:9092 --topic data.input.x &
echo '{"customer":"b"}' | jq -rc | kafka-console-producer --bootstrap-server big-host-1.datadisorder.dev:9092 --topic data.input.y &
echo '{"customer":"c"}' | jq -rc | kafka-console-producer --bootstrap-server big-host-1.datadisorder.dev:9092 --topic data.input.z &
echo '{"customer":"x"}' | jq -rc | kafka-console-producer --bootstrap-server big-host-1.datadisorder.dev:9092 --topic data.input.x &
echo '{"no":"value"}' | jq -rc | kafka-console-producer --bootstrap-server big-host-1.datadisorder.dev:9092 --topic data.input.y &
```

Validate the messages were parsed using the Confluent Control Center or with kafkacat

