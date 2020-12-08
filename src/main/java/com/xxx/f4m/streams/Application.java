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

import com.sun.net.httpserver.HttpHandler;
import com.xxx.f4m.streams.topologies.MergeTopicsTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
import java.util.Properties;
import java.util.function.Supplier;

/**
 * This Kafka stream allows to merge event coming from several topics in one target topic
 */
/// todo auto restart on new topic
public class Application {
    private final static Logger log = LoggerFactory.getLogger(Application.class);
    private static final int PORT = 8089;
    private static String sourceTopicsPattern;
    private static String targetTopic;

    private static Properties asProperties(Config config) {
        var props = new Properties();

        config.entrySet().forEach(e -> {
            String k = e.getKey();
            Object v = e.getValue().unwrapped();

            props.put(k, v);
        });
        return props;
    }

    private static HttpServer createHttpServer(int port) {
        try {
            return HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            log.error("Impossible to create HTTP server: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static PrometheusMeterRegistry createPrometheusMeterRegistry(KafkaStreams streams) {
        var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new KafkaStreamsMetrics(streams).bindTo(registry);
        return registry;
    }

    public static void main(String[] args) {
        log.info("Starting stream ...");

        var appConfig = ConfigFactory.load();
        var streamsProperties = asProperties(appConfig.getConfig("streams"));
        var mergeTopicConfigs = appConfig.getConfig("topologies.merge-topics");

        sourceTopicsPattern = mergeTopicConfigs.getString("topics.pattern.target");
        targetTopic = mergeTopicConfigs.getString("topic.target");
        
        var topology = new MergeTopicsTopology(sourceTopicsPattern,targetTopic).topology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsProperties);

        // catch and log tricky exceptions
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            log.error("Something bad happened in thread {}: {}", thread, throwable.getMessage());
            streams.close(Duration.of(3, ChronoUnit.SECONDS));
            System.exit(1);
        });

        streams.setStateListener((before, after) -> log.debug("Switching from state {} to {}", before, after));

        // Print the topology description
        log.info(topology.describe().toString());

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
            // Probe ready (si pas de store ready == live ou sinon store running)
            Supplier<Boolean> probe = () -> streams.state().isRunningOrRebalancing();
            var server = createHttpServer(PORT);
            server.createContext("/", infoHandler(topology));
            server.createContext("/describe", describeHandler(topology));
            server.createContext("/metrics", metricsHandler(createPrometheusMeterRegistry(streams)));
            server.createContext("/ready", readinessHandler(probe));
            server.createContext("/live", livenessHandler(probe));
            server.start();
            log.info("Serving metrics on: http://{}:{}/metrics", server.getAddress().getHostName(), 8080);
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static HttpHandler metricsHandler(PrometheusMeterRegistry registry) {
        return exchange -> {
            String response = registry.scrape();
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        };
    }

    public static HttpHandler readinessHandler(Supplier<Boolean> probe) {
        return exchange -> {
            exchange.sendResponseHeaders((probe.get()) ? 200 : 500, 0);
            exchange.close();
        };
    }

    public static HttpHandler livenessHandler(Supplier<Boolean> probe) {
        return exchange -> {
            exchange.sendResponseHeaders((probe.get()) ? 200 : 500, 0);
            exchange.close();
        };
    }

    public static HttpHandler infoHandler(Topology topology) {
        return exchange -> {
            String response = "<p>This stream allows to merge topics according to the pattern + " + sourceTopicsPattern + " + in a topic named " + targetTopic  + "</p>" +
                    "<a href=\"/metrics\">metrics</a><hr>" +
                    "<a href=\"/liveness\">liveness</a><hr>" +
                    "<a href=\"/readiness\">readiness</a><hr>" +
                    "<a href=\"/describe\">describe</a><hr>";
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        };
    }
    public static HttpHandler describeHandler(Topology topology) {
        return exchange -> {
            String response = topology.describe().toString();
            exchange.sendResponseHeaders(200, response.getBytes().length);
            try (var os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        };
    }
}
