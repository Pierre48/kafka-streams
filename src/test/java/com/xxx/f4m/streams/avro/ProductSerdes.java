package com.xxx.f4m.streams.avro;

import com.xxx.avro.Product;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class ProductSerdes {

    public final Serde<Product> purchaseOrderValueSerde;

    public ProductSerdes(String registryUrl) {
        purchaseOrderValueSerde = new SpecificAvroSerde<>();
        purchaseOrderValueSerde.configure(Map.of(
                SCHEMA_REGISTRY_URL_CONFIG, registryUrl
                ),
                false);
    }

    public ProductSerdes(Properties properties) {
        this((String) properties.getOrDefault(SCHEMA_REGISTRY_URL_CONFIG, "no-schema-registry"));
    }
}
