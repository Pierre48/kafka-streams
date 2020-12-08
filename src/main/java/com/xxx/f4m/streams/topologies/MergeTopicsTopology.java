package com.xxx.f4m.streams.topologies;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * A kafka stream used to merge a set of topic, given by a regex expression,
 * into a single topic
 */
public class MergeTopicsTopology {
    public static final Logger logger = LoggerFactory.getLogger(MergeTopicsTopology.class);
    private final String sourceTopicsPattern;
    private final String targetTopic;

    public MergeTopicsTopology(String sourceTopicsPattern, String targetTopic) {
        this.sourceTopicsPattern = sourceTopicsPattern;
        this.targetTopic = targetTopic;
    }
    /**
     * Build the topology
     * @return The topology
     */
    public Topology topology() {
        final StreamsBuilder builder = new StreamsBuilder();
        var pattern = Pattern.compile(sourceTopicsPattern);
        if (pattern.matcher(targetTopic).matches())
            throw new IllegalArgumentException("Source topic pattern match with target topic name. That is not possible (infinite loop) !");
        builder.stream(pattern).to(targetTopic);
        return builder.build();
    }
}
