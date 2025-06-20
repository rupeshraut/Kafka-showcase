package com.example.kafka.streams;

import com.example.kafka.config.KafkaConfigFactory;
import com.example.kafka.avro.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Kafka Streams demonstration showcasing:
 * - Stream processing with exactly-once semantics
 * - Basic transformations and filtering
 * - Windowing and aggregations
 */
public class StreamsDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(StreamsDemo.class);
    private static final String APPLICATION_ID = "kafka-showcase-streams";
    
    // Topic names
    private static final String USER_EVENTS_TOPIC = "user-events";
    private static final String USER_ANALYTICS_TOPIC = "user-analytics";
    
    private KafkaStreams streams;
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    public static void main(String[] args) {
        StreamsDemo demo = new StreamsDemo();
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered, stopping streams...");
            demo.stop();
        }));
        
        try {
            demo.start();
            // Keep the application running
            Thread.currentThread().join();
        } catch (Exception e) {
            logger.error("Error in streams demo", e);
        }
    }
    
    /**
     * Start the Kafka Streams application
     */
    public void start() {
        Properties props = KafkaConfigFactory.createStreamsConfig(APPLICATION_ID);
        Topology topology = buildTopology();
        
        streams = new KafkaStreams(topology, props);
        
        // Set up state listener for monitoring
        streams.setStateListener((newState, oldState) -> {
            logger.info("Stream state transition: {} -> {}", oldState, newState);
        });
        
        logger.info("Starting Kafka Streams application: {}", APPLICATION_ID);
        streams.start();
        
        logger.info("Topology description:\n{}", topology.describe());
    }
    
    /**
     * Build the Kafka Streams topology
     */
    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        
        // Create streams
        KStream<String, String> userEventsStream = builder.stream(USER_EVENTS_TOPIC);
        
        // Convert JSON strings to objects for processing
        KStream<String, UserEvent> events = userEventsStream
            .mapValues(this::parseUserEvent)
            .filter((key, event) -> event != null);
        
        // Process events
        buildEventProcessingPipeline(events);
        buildAggregationPipeline(events);
        
        return builder.build();
    }
    
    /**
     * Process user events with filtering and enrichment
     */
    private void buildEventProcessingPipeline(KStream<String, UserEvent> events) {
        events
            .filter((key, event) -> event.getEventType() != null)
            .filter((key, event) -> event.getTimestamp() > Instant.now().minus(Duration.ofDays(30)).toEpochMilli())
            .mapValues(this::enrichEvent)
            .peek((key, event) -> logger.debug("Processing event: {} for user: {}", 
                event.getEventType(), event.getUserId()))
            .mapValues(this::eventToJson)
            .to(USER_ANALYTICS_TOPIC);
    }
    
    /**
     * Build aggregation pipeline for real-time analytics
     */
    private void buildAggregationPipeline(KStream<String, UserEvent> events) {
        // Count events by type in tumbling windows
        events
            .groupBy((key, event) -> event.getEventType().toString())
            .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
            .count()
            .toStream()
            .peek((windowedKey, count) -> 
                logger.info("Event type {} count in window: {}", windowedKey.key(), count))
            .selectKey((windowedKey, count) -> windowedKey.key())
            .mapValues(count -> count.toString())
            .to("event-counts");
    }
    
    /**
     * Parse JSON string to UserEvent object
     */
    private UserEvent parseUserEvent(String json) {
        try {
            return objectMapper.readValue(json, UserEvent.class);
        } catch (Exception e) {
            logger.error("Failed to parse user event JSON: {}", json, e);
            return null;
        }
    }
    
    /**
     * Enrich event with additional metadata
     */
    private UserEvent enrichEvent(UserEvent event) {
        // Add processing timestamp
        if (event.getMetadata() != null) {
            event.getMetadata().put("processed_at", Instant.now().toString());
        }
        return event;
    }
    
    /**
     * Convert UserEvent to JSON string
     */
    private String eventToJson(UserEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            logger.error("Failed to convert event to JSON", e);
            return "{}";
        }
    }
    
    /**
     * Stop the Kafka Streams application
     */
    public void stop() {
        if (streams != null) {
            logger.info("Stopping Kafka Streams application");
            streams.close(Duration.ofSeconds(30));
            logger.info("Kafka Streams application stopped");
        }
    }
}
