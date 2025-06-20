package com.example.kafka.transactions;

import com.example.kafka.config.KafkaConfigFactory;
import com.example.kafka.avro.UserEvent;
import com.example.kafka.avro.EventType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Demonstrates advanced Kafka transaction patterns including:
 * - Exactly-once semantics with transactions
 * - Consume-Transform-Produce pattern
 * - Error handling and recovery
 * - Read committed isolation level
 */
public class TransactionDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionDemo.class);
    
    private static final String INPUT_TOPIC = "user-events";
    private static final String OUTPUT_TOPIC = "processed-events";
    private static final String CONSUMER_GROUP = "transaction-processor";
    
    private final KafkaProducer<String, Object> producer;
    private final KafkaConsumer<String, Object> consumer;
    private volatile boolean running = true;
    
    public TransactionDemo() {
        // Initialize transactional producer
        this.producer = new KafkaProducer<>(KafkaConfigFactory.createExactlyOnceProducerConfig());
        producer.initTransactions();
        
        // Initialize consumer with read_committed isolation
        this.consumer = new KafkaConsumer<>(KafkaConfigFactory.createExactlyOnceConsumerConfig(CONSUMER_GROUP));
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
        
        logger.info("TransactionDemo initialized with exactly-once semantics");
    }
    
    public static void main(String[] args) {
        TransactionDemo demo = new TransactionDemo();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered, stopping transaction demo...");
            demo.stop();
        }));
        
        try {
            demo.runConsumeTransformProduce();
        } catch (Exception e) {
            logger.error("Error in transaction demo", e);
        } finally {
            demo.close();
        }
    }
    
    /**
     * Demonstrates the consume-transform-produce pattern with transactions
     */
    public void runConsumeTransformProduce() {
        logger.info("Starting consume-transform-produce loop with transactions");
        
        while (running) {
            try {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
                
                if (!records.isEmpty()) {
                    processRecordsTransactionally(records);
                }
                
            } catch (Exception e) {
                logger.error("Error in consume-transform-produce loop", e);
                handleError(e);
            }
        }
    }
    
    /**
     * Process a batch of records within a single transaction
     */
    private void processRecordsTransactionally(ConsumerRecords<String, Object> records) {
        producer.beginTransaction();
        
        try {
            Map<String, List<UserEvent>> userEvents = new HashMap<>();
            
            // Process each record
            for (ConsumerRecord<String, Object> record : records) {
                UserEvent processedEvent = transformRecord(record);
                if (processedEvent != null) {
                    // Group events by user for batch processing
                    String userId = processedEvent.getUserId().toString();
                    userEvents.computeIfAbsent(userId, k -> new ArrayList<>()).add(processedEvent);
                }
            }
            
            // Produce transformed records
            for (Map.Entry<String, List<UserEvent>> entry : userEvents.entrySet()) {
                String userId = entry.getKey();
                List<UserEvent> events = entry.getValue();
                
                // Process user's events as a batch
                UserEvent aggregatedEvent = aggregateUserEvents(userId, events);
                
                ProducerRecord<String, Object> outputRecord = new ProducerRecord<>(
                    OUTPUT_TOPIC, userId, aggregatedEvent);
                
                producer.send(outputRecord);
                logger.debug("Produced aggregated event for user: {}", userId);
            }
            
            // Commit consumer offsets to the transaction
            producer.sendOffsetsToTransaction(
                getOffsetsToCommit(records), 
                consumer.groupMetadata());
            
            // Commit the transaction
            producer.commitTransaction();
            
            logger.info("Successfully processed {} records in transaction", records.count());
            
        } catch (Exception e) {
            logger.error("Error processing records, aborting transaction", e);
            producer.abortTransaction();
            throw e;
        }
    }
    
    /**
     * Transform a single input record
     */
    private UserEvent transformRecord(ConsumerRecord<String, Object> record) {
        try {
            if (!(record.value() instanceof UserEvent)) {
                logger.warn("Expected UserEvent but got: {}", record.value().getClass());
                return null;
            }
            
            UserEvent originalEvent = (UserEvent) record.value();
            
            // Create transformed event
            UserEvent transformedEvent = UserEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setUserId(originalEvent.getUserId())
                .setEventType(originalEvent.getEventType())
                .setTimestamp(Instant.now().toEpochMilli())
                .setSessionId(originalEvent.getSessionId())
                .setMetadata(enrichMetadata(originalEvent.getMetadata()))
                .build();
            
            logger.debug("Transformed event: {} -> {}", originalEvent.getEventId(), 
                transformedEvent.getEventId());
            
            return transformedEvent;
            
        } catch (Exception e) {
            logger.error("Failed to transform record: {}", record, e);
            return null;
        }
    }
    
    /**
     * Aggregate multiple events for the same user
     */
    private UserEvent aggregateUserEvents(String userId, List<UserEvent> events) {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("Cannot aggregate empty event list");
        }
        
        // Find the most recent event as base
        UserEvent baseEvent = events.stream()
            .max(Comparator.comparing(UserEvent::getTimestamp))
            .orElse(events.get(0));
        
        // Create aggregated metadata
        Map<String, String> aggregatedMetadata = new HashMap<>();
        aggregatedMetadata.put("aggregated_count", String.valueOf(events.size()));
        aggregatedMetadata.put("event_types", events.stream()
            .map(e -> e.getEventType().toString())
            .distinct()
            .reduce((a, b) -> a + "," + b)
            .orElse(""));
        aggregatedMetadata.put("processed_at", Instant.now().toString());
        
        // Copy metadata from base event
        if (baseEvent.getMetadata() != null) {
            aggregatedMetadata.putAll(baseEvent.getMetadata());
        }
        
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(Long.parseLong(userId))
            .setEventType(determineAggregateEventType(events))
            .setTimestamp(Instant.now().toEpochMilli())
            .setSessionId(baseEvent.getSessionId())
            .setMetadata(aggregatedMetadata)
            .build();
    }
    
    /**
     * Determine the aggregate event type based on input events
     */
    private EventType determineAggregateEventType(List<UserEvent> events) {
        // Simple logic: if there's a purchase event, prioritize it
        boolean hasPurchase = events.stream().anyMatch(e -> e.getEventType() == EventType.PURCHASE);
        if (hasPurchase) {
            return EventType.PURCHASE;
        }
        
        // Otherwise, use the most common event type
        return events.stream()
            .collect(java.util.stream.Collectors.groupingBy(
                UserEvent::getEventType,
                java.util.stream.Collectors.counting()))
            .entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(EventType.LOGIN);
    }
    
    /**
     * Enrich metadata with processing information
     */
    private Map<String, String> enrichMetadata(Map<String, String> originalMetadata) {
        Map<String, String> enriched = new HashMap<>();
        
        if (originalMetadata != null) {
            enriched.putAll(originalMetadata);
        }
        
        enriched.put("processed_by", "TransactionDemo");
        enriched.put("processed_at", Instant.now().toString());
        enriched.put("processing_version", "1.0");
        
        return enriched;
    }
    
    /**
     * Get offsets to commit from consumed records
     */
    private Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> 
            getOffsetsToCommit(ConsumerRecords<String, Object> records) {
        
        Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets = new HashMap<>();
        
        for (org.apache.kafka.common.TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, Object>> partitionRecords = records.records(partition);
            if (!partitionRecords.isEmpty()) {
                ConsumerRecord<String, Object> lastRecord = partitionRecords.get(partitionRecords.size() - 1);
                offsets.put(partition, 
                    new org.apache.kafka.clients.consumer.OffsetAndMetadata(lastRecord.offset() + 1));
            }
        }
        
        return offsets;
    }
    
    /**
     * Handle errors in processing
     */
    private void handleError(Exception e) {
        logger.error("Handling error in transaction processing", e);
        
        // Implement retry logic, dead letter queue, or other error handling
        try {
            Thread.sleep(5000); // Simple backoff
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
    
    /**
     * Stop the transaction demo
     */
    public void stop() {
        running = false;
        consumer.wakeup();
        logger.info("Transaction demo stop requested");
    }
    
    /**
     * Clean up resources
     */
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Producer closed");
        }
        if (consumer != null) {
            consumer.close();
            logger.info("Consumer closed");
        }
    }
}
