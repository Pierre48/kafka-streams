package com.xxx.f4m.streams.topologies;

import com.xxx.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MergeTopicsInfiniteLoopTests {

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


    @Test
    public void ThreeEventInOneTopicTest() throws Exception {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(()->new MergeTopicsTopology("^test.*$","test").topology());
    }
}
