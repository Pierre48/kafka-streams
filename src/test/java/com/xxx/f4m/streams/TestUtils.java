package com.xxx.f4m.streams;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xxx.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class TestUtils {

    public static final Path RESOURCES_DIR = Paths.get("src/test/resources");
    public static final ObjectMapper MAPPER = new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);

    public static final KeyValue<String, Product> loadPurchaseOrderRecord(String filename) throws IOException {
        File file = RESOURCES_DIR.resolve(filename).toFile();
        var record = MAPPER.readValue(file, Map.class);
        var key = (String) record.get("key");
        var value = MAPPER.convertValue(record.get("value"), Product.class);
        return new KeyValue<>(key, value);
    }
}
