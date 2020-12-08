/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xxx.f4m.streams;

import com.xxx.f4m.streams.topologies.MergeTopicsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.streams.StreamsConfig;

import java.util.concurrent.CountDownLatch;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This Kafka stream allows to merge event coming from several topics in one target topic
 */
public class Application {
    private final static Logger log = LoggerFactory.getLogger(Application.class);
    public static final int PORT = 8080;


    public static Properties asProperties(Config config) {
        var props = new Properties();

        config.entrySet().forEach(e -> {
            String k = e.getKey();
            Object v = e.getValue().unwrapped();

            props.put(k, v);
        });

        return props;
    }

    public static Map<String, Object> asMap(Config config) {
        var map = new HashMap<String, Object>();

        config.entrySet().forEach(e -> {
            String k = e.getKey();
            Object v = e.getValue().unwrapped();

            map.put(k, v);
        });

        return map;
    }

    public static HttpServer createHttpServer(int port) {
        try {
            return HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            log.error("Impossible to create HTTP server: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static PrometheusMeterRegistry createPrometheusMeterRegistry(KafkaStreams streams) {
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new KafkaStreamsMetrics(streams).bindTo(registry);
        return registry;
    }














    private static String targetTopic;
    private static String sourceTopicsPattern;
    private static Properties streamProps;

    public static void main(String[] args) {
        log.info("Starting stream ...");

        getInputParameters();
        /// todo auto restart on new topic

        var topology = new MergeTopicsTopology(sourceTopicsPattern,targetTopic ).topology();
        final KafkaStreams streams = new KafkaStreams(topology, streamProps);
        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void getInputParameters() {
        streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, getEnvValue("APPLICATION_ID_CONFIG","sample-stream-merge-topics"));
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getEnvValue("BOOTSTRAP_SERVERS_CONFIG","localhost:9092"));
//        streamProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, getEnvValue("DEFAULT_KEY_SERDE_CLASS_CONFIG",Serdes.String().getClass().toString()));
//        streamProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, getEnvValue("DEFAULT_VALUE_SERDE_CLASS_CONFIG",Serdes.String().getClass().toString()));

        Properties kafkaProps = new Properties();
        kafkaProps.put(StreamsConfig.APPLICATION_ID_CONFIG, getEnvValue("APPLICATION_ID_CONFIG","sample-stream-merge-topics"));
        kafkaProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getEnvValue("BOOTSTRAP_SERVERS_CONFIG","localhost:9092"));
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        sourceTopicsPattern = getEnvValue("SOURCE_TOPIC_PATTERN",null);
        if (sourceTopicsPattern==null || "".equals(sourceTopicsPattern)) throw new IllegalArgumentException("Parameter SOURCE_TOPIC_PATTERN must be set");

        targetTopic = getEnvValue("TARGET_TOPIC",null);
        if (targetTopic==null || "".equals(targetTopic)) throw new IllegalArgumentException("Parameter TARGET_TOPIC must be set, and must contains at least 2 topic names");

    }

    public static String getEnvValue(String environmentKey, String defaultValue)
    {
        String envValue = System.getenv(environmentKey);
        if(envValue != null && !envValue.isEmpty())
        {
            return envValue;
        }
        return defaultValue;
    }
}
