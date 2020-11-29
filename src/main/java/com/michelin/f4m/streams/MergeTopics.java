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
package com.michelin.f4m.streams;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

/**
 * This Kafka stream allows to merge event comming from several topics in one target topic
 */
public class MergeTopics {
    final static Logger logger = LoggerFactory.getLogger(MergeTopics.class);
    public static void main(String[] args) {
        logger.info("Starting stream ...");

        Properties props = getProperties();
        String sourceTopicPattern = System.getenv("SOURCE_TOPIC_PATTERN");
        if (sourceTopicPattern==null || "".equals(sourceTopicPattern)) throw new IllegalArgumentException("Parameter SOURCE_TOPIC_PATTERN must be set, and must contains at least 2 topic names");

        String[] sourceTopics = GetSourceTopics(props,sourceTopicPattern);
        if (sourceTopics==null || sourceTopics.length <= 1) throw new IllegalArgumentException("Parameter sourceTopics must be set, and must contains at least 2 topic names");

        String targetTopic = System.getenv("TARGET_TOPIC");
        if (targetTopic==null || "".equals(targetTopic)) throw new IllegalArgumentException("Parameter TARGET_TOPIC must be set, and must contains at least 2 topic names");

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<Byte[], Byte[]> stream = builder.stream(sourceTopics[0]);
        for (int i = 1; i < sourceTopics.length; i++) {
            logger.debug("Adding " + sourceTopics[i] + " to the kafka stream topology");
            KStream<Byte[], Byte[]> streamToMerge = builder.stream(sourceTopics[i]);
            stream = stream.merge(streamToMerge);
        }
        stream.to(targetTopic);

        final Topology topology = builder.build();
        logger.info("Topology : " + topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
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

    /**
     * Return properties to connect to a kafka cluster
     * @return List of properties
     */
    private static Properties getProperties() {
        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    /**
     * Return an array of topic name according to a regex
     * @param props Allows to connect to a kaka cluster
     * @param sourceTopicPattern a regex pattern that allows to filter topic name
     * @return A list of topic name
     */
    private static String[] GetSourceTopics(Properties props, String sourceTopicPattern) {
        ArrayList<String> result=new ArrayList<String>();

        Pattern p = Pattern.compile(sourceTopicPattern);

        KafkaConsumer<String, String> consumer=null;
        try {
            consumer = new KafkaConsumer(props);
            for (String topic : consumer.listTopics().keySet())
                if (p.matcher(topic).matches())
                    result.add(topic);
        }
        finally {
            if (consumer!=null)
                consumer.close();
        }

        return result.toArray(new String[0]);
    }
}
