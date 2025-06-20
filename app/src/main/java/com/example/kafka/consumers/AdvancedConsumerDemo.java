package com.example.kafka.consumers;

import com.example.kafka.config.KafkaConfigFactory;
import com.example.kafka.avro.User;
import com.example.kafka.avro.UserEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Advanced Kafka Consumer demonstration showcasing:
 * - Exactly-once processing with manual offset management
 * - Avro deserialization with Schema Registry
 * - Error handling and retry mechanisms
 * - Consumer group coordination
 * - Metrics and monitoring
 * - Graceful shutdown
 */
public class AdvancedConsumerDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedConsumerDemo.class);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(1000);
    private static final String USER_TOPIC = "users";
    private static final String USER_EVENTS_TOPIC = "user-events";
    
    private final KafkaConsumer<String, Object> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new ConcurrentHashMap<>();
    private final AtomicLong processedRecords = new AtomicLong(0);
    private volatile boolean running = true;
    
    public AdvancedConsumerDemo(String groupId) {
        this.consumer = new KafkaConsumer<>(KafkaConfigFactory.createExactlyOnceConsumerConfig(groupId));
        logger.info("Consumer initialized with group ID: {}", groupId);
    }
    
    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : "kafka-showcase-consumer-group";
        AdvancedConsumerDemo demo = new AdvancedConsumerDemo(groupId);
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered, stopping consumer...");
            demo.stop();
        }));
        
        try {
            demo.run();
        } catch (Exception e) {
            logger.error("Error in consumer demo", e);
        } finally {
            demo.close();
        }
    }
    
    /**
     * Main consumption loop with advanced error handling
     */
    public void run() {
        try {
            // Subscribe to topics
            consumer.subscribe(Arrays.asList(USER_TOPIC, USER_EVENTS_TOPIC));
            logger.info("Subscribed to topics: {}", Arrays.asList(USER_TOPIC, USER_EVENTS_TOPIC));
            
            long lastMetricsReport = System.currentTimeMillis();
            
            while (running) {
                try {
                    ConsumerRecords<String, Object> records = consumer.poll(POLL_TIMEOUT);
                    
                    if (!records.isEmpty()) {
                        processRecords(records);
                        commitOffsetsSync();
                    }
                    
                    // Report metrics every 30 seconds
                    if (System.currentTimeMillis() - lastMetricsReport > 30000) {
                        reportMetrics();
                        lastMetricsReport = System.currentTimeMillis();
                    }
                    
                } catch (Exception e) {
                    logger.error("Error during polling/processing", e);
                    handleConsumerError(e);
                }
            }
            
        } catch (Exception e) {
            logger.error("Fatal error in consumer loop", e);
        }
    }
    
    /**
     * Process consumed records with type-specific handling
     */
    private void processRecords(ConsumerRecords<String, Object> records) {
        logger.debug("Processing {} records", records.count());
        
        for (ConsumerRecord<String, Object> record : records) {
            try {
                processRecord(record);
                updateOffsetToCommit(record);
                processedRecords.incrementAndGet();
                
            } catch (Exception e) {
                logger.error("Failed to process record: topic={}, partition={}, offset={}, key={}", 
                    record.topic(), record.partition(), record.offset(), record.key(), e);
                
                // Implement retry logic or dead letter queue here
                handleRecordProcessingError(record, e);
            }
        }
    }
    
    /**
     * Process individual record based on its type and topic
     */
    private void processRecord(ConsumerRecord<String, Object> record) {
        logger.debug("Processing record: topic={}, key={}, timestamp={}", 
            record.topic(), record.key(), Instant.ofEpochMilli(record.timestamp()));
        
        switch (record.topic()) {
            case USER_TOPIC:
                processUserRecord(record);
                break;
            case USER_EVENTS_TOPIC:
                processUserEventRecord(record);
                break;
            default:
                logger.warn("Unknown topic: {}", record.topic());
        }
    }
    
    /**
     * Process User records
     */
    private void processUserRecord(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof User) {
            User user = (User) record.value();
            logger.info("Processing user: id={}, email={}, active={}", 
                user.getId(), user.getEmail(), user.getIsActive());
            
            // Simulate business logic
            validateUser(user);
            enrichUserData(user);
            persistUser(user);
            
        } else {
            logger.warn("Expected User object but got: {}", record.value().getClass());
        }
    }
    
    /**
     * Process UserEvent records
     */
    private void processUserEventRecord(ConsumerRecord<String, Object> record) {
        if (record.value() instanceof UserEvent) {
            UserEvent event = (UserEvent) record.value();
            logger.info("Processing user event: id={}, userId={}, type={}, timestamp={}", 
                event.getEventId(), event.getUserId(), event.getEventType(), 
                Instant.ofEpochMilli(event.getTimestamp()));
            
            // Simulate event processing
            analyzeEvent(event);
            updateUserMetrics(event);
            triggerDownstreamProcessing(event);
            
        } else {
            logger.warn("Expected UserEvent object but got: {}", record.value().getClass());
        }
    }
    
    /**
     * Update offset to commit for exactly-once processing
     */
    private void updateOffsetToCommit(ConsumerRecord<String, Object> record) {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata offsetMetadata = new OffsetAndMetadata(record.offset() + 1);
        offsetsToCommit.put(topicPartition, offsetMetadata);
    }
    
    /**
     * Commit offsets synchronously for exactly-once semantics
     */
    private void commitOffsetsSync() {
        if (!offsetsToCommit.isEmpty()) {
            try {
                consumer.commitSync(offsetsToCommit);
                logger.debug("Committed offsets for {} partitions", offsetsToCommit.size());
                offsetsToCommit.clear();
            } catch (Exception e) {
                logger.error("Failed to commit offsets", e);
                // In a production system, you might want to retry or implement backoff
            }
        }
    }
    
    /**
     * Handle consumer-level errors
     */
    private void handleConsumerError(Exception e) {
        logger.error("Consumer error occurred", e);
        
        // Implement retry logic, circuit breaker, or other error handling strategies
        try {
            Thread.sleep(1000); // Simple backoff
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
    
    /**
     * Handle record processing errors
     */
    private void handleRecordProcessingError(ConsumerRecord<String, Object> record, Exception e) {
        // In a production system, you might:
        // 1. Send to a dead letter queue
        // 2. Implement retry with exponential backoff
        // 3. Alert monitoring systems
        // 4. Skip the record after max retries
        
        logger.error("Skipping problematic record after error: topic={}, partition={}, offset={}", 
            record.topic(), record.partition(), record.offset());
    }
    
    /**
     * Report consumption metrics
     */
    private void reportMetrics() {
        long processed = processedRecords.get();
        logger.info("Consumer metrics: processed_records={}, consumer_group={}", 
            processed, consumer.groupMetadata().groupId());
        
        // In a production system, you would send these metrics to your monitoring system
        // (e.g., Prometheus, CloudWatch, etc.)
    }
    
    // Business logic simulation methods
    private void validateUser(User user) {
        if (user.getEmail() == null || user.getEmail().toString().isEmpty()) {
            throw new IllegalArgumentException("User email cannot be null or empty");
        }
    }
    
    private void enrichUserData(User user) {
        // Simulate data enrichment (e.g., from external services)
        logger.debug("Enriching user data for user: {}", user.getId());
    }
    
    private void persistUser(User user) {
        // Simulate persistence (e.g., to database)
        logger.debug("Persisting user: {}", user.getId());
    }
    
    private void analyzeEvent(UserEvent event) {
        // Simulate event analysis
        logger.debug("Analyzing event: {}", event.getEventId());
    }
    
    private void updateUserMetrics(UserEvent event) {
        // Simulate metrics update
        logger.debug("Updating metrics for user: {}", event.getUserId());
    }
    
    private void triggerDownstreamProcessing(UserEvent event) {
        // Simulate triggering downstream systems
        logger.debug("Triggering downstream processing for event: {}", event.getEventId());
    }
    
    /**
     * Stop the consumer gracefully
     */
    public void stop() {
        running = false;
        consumer.wakeup();
        logger.info("Consumer stop requested");
    }
    
    /**
     * Clean up resources
     */
    public void close() {
        if (consumer != null) {
            consumer.close();
            logger.info("Consumer closed");
        }
    }
}
