package com.xxx.f4m.streams.topologies;

import com.xxx.avro.Product;
import com.xxx.f4m.streams.avro.ProductSerDes;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class MergeTopicsTests {

    public static final String MOCK_SOURCE_TOPIC_1 = "test_1";
    public static final String MOCK_SOURCE_TOPIC_2 = "test_2";
    public static final String MOCK_SOURCE_TOPIC_3 = "test_3";
    public static final String MOCK_TARGET_TOPIC = "test";

    public static final Map<String, Object> MOCK_CONFIG = Map.of(
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234",
        StreamsConfig.APPLICATION_ID_CONFIG, "merge-topics-stream-app-test",
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://schema-registry",
        StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName(),
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName()
    );

    public static final Serde<String> STRING_SERDE = Serdes.String();
    TopologyTestDriver testDriver;
    TestInputTopic<String, Product> testTopic1;
    TestInputTopic<String, Product> testTopic2;
    TestInputTopic<String, Product> testTopic3;
    TestOutputTopic<String, Product> outputTopic;

    @BeforeEach
    public void beforeEach() throws Exception {
        var config = new Properties();
        config.putAll(MOCK_CONFIG);

        var productSerDe = new ProductSerDes(config).purchaseOrderValueSerde;

        var topology = new MergeTopicsTopology("^test_.*$","test").topology();

        testDriver = new TopologyTestDriver(topology, config);

        testTopic1 = testDriver.createInputTopic(MOCK_SOURCE_TOPIC_1, STRING_SERDE.serializer(), productSerDe.serializer());
        testTopic2 = testDriver.createInputTopic(MOCK_SOURCE_TOPIC_2, STRING_SERDE.serializer(), productSerDe.serializer());
        testTopic3 = testDriver.createInputTopic(MOCK_SOURCE_TOPIC_3, STRING_SERDE.serializer(), productSerDe.serializer());
        outputTopic = testDriver.createOutputTopic(MOCK_TARGET_TOPIC, STRING_SERDE.deserializer(), productSerDe.deserializer()

        );
    }

    @AfterEach
    public void afterEach() {
        testDriver.close();
    }

    //https://www.confluent.io/blog/stream-processing-part-2-testing-your-streaming-application/
    @Test
    public void OneEventTest() throws Exception {

        testTopic1.pipeInput(MOCK_SOURCE_TOPIC_1,new Product("banana","yellow",1));
        assertThat(outputTopic.isEmpty()).isFalse();
        var outputRecords = outputTopic.readKeyValuesToList();
        var firstRecord = outputRecords.get(0);
        assertThat(firstRecord.value.getName().equals("banana"));
    }
    @Test
    public void ThreeEventInThreeTopicTest() throws Exception {

        testTopic1.pipeInput(MOCK_SOURCE_TOPIC_1,new Product("banana","yellow",1));
        testTopic2.pipeInput(MOCK_SOURCE_TOPIC_2,new Product("Apple","green",1));
        testTopic3.pipeInput(MOCK_SOURCE_TOPIC_3,new Product("strawberry","red",1));
        assertThat(outputTopic.isEmpty()).isFalse();
        var outputRecords = outputTopic.readKeyValuesToList();
        assertThat(outputRecords.stream().count()==3);
    }

    @Test
    public void ThreeEventInOneTopicTest() throws Exception {

        testTopic1.pipeInput(MOCK_SOURCE_TOPIC_1,new Product("banana","yellow",1));
        testTopic1.pipeInput(MOCK_SOURCE_TOPIC_1,new Product("Apple","green",1));
        testTopic1.pipeInput(MOCK_SOURCE_TOPIC_1,new Product("strawberry","red",1));
        assertThat(outputTopic.isEmpty()).isFalse();
        var outputRecords = outputTopic.readKeyValuesToList();
        assertThat(outputRecords.stream().count()==3);
    }
}
