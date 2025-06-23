package com.example.kafka.producers;

import com.example.kafka.config.KafkaConfigFactory;
import com.example.kafka.avro.User;
import com.example.kafka.avro.UserEvent;
import com.example.kafka.avro.EventType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Advanced Kafka Producer demonstration showcasing:
 * - Exactly-once semantics with transactions
 * - Avro serialization with Schema Registry
 * - Asynchronous and synchronous production
 * - Error handling and retry mechanisms
 * - Metrics and monitoring
 */
public class AdvancedProducerDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedProducerDemo.class);
    private static final String USER_TOPIC = "users";
    private static final String USER_EVENTS_TOPIC = "user-events";
    
    private final KafkaProducer<String, Object> producer;
    private final Random random = new Random();
    
    public AdvancedProducerDemo() {
        this.producer = new KafkaProducer<>(KafkaConfigFactory.createExactlyOnceProducerConfig());
        
        // Initialize transactions for exactly-once semantics
        producer.initTransactions();
        logger.info("Producer initialized with exactly-once semantics");
    }
    
    public static void main(String[] args) {
        AdvancedProducerDemo demo = new AdvancedProducerDemo();
        
        try {
            // Demonstrate various production patterns
            demo.demonstrateBasicProduction();
            demo.demonstrateTransactionalProduction();
            demo.demonstrateAsynchronousProduction();
            demo.demonstrateBatchProduction();
            demo.demonstrateAdvancedPartitioning();
            
        } catch (Exception e) {
            logger.error("Error in producer demo", e);
        } finally {
            demo.close();
        }
    }
    
    /**
     * Demonstrates basic record production with Avro serialization
     */
    public void demonstrateBasicProduction() throws ExecutionException, InterruptedException {
        logger.info("=== Demonstrating Basic Production ===");
        
        User user = createSampleUser();
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            USER_TOPIC,
            user.getId().toString(),
            user
        );
        
        // Synchronous send with error handling
        try {
            RecordMetadata metadata = producer.send(record).get();
            logger.info("Record sent successfully: topic={}, partition={}, offset={}, timestamp={}", 
                metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
        } catch (Exception e) {
            logger.error("Failed to send record", e);
            throw e;
        }
    }
    
    /**
     * Demonstrates transactional production for exactly-once semantics
     */
    public void demonstrateTransactionalProduction() {
        logger.info("=== Demonstrating Transactional Production ===");
        
        producer.beginTransaction();
        try {
            // Send multiple related records in a single transaction
            User user = createSampleUser();
            UserEvent event = createUserEvent(user.getId(), EventType.PROFILE_UPDATE);
            
            ProducerRecord<String, Object> userRecord = new ProducerRecord<>(
                USER_TOPIC, user.getId().toString(), user);
            ProducerRecord<String, Object> eventRecord = new ProducerRecord<>(
                USER_EVENTS_TOPIC, event.getEventId().toString(), event);
            
            producer.send(userRecord);
            producer.send(eventRecord);
            
            // Simulate some processing that could fail
            if (random.nextDouble() < 0.1) { // 10% chance of failure
                throw new RuntimeException("Simulated processing error");
            }
            
            producer.commitTransaction();
            logger.info("Transaction committed successfully");
            
        } catch (Exception e) {
            logger.error("Transaction failed, rolling back", e);
            producer.abortTransaction();
        }
    }
    
    /**
     * Demonstrates asynchronous production with callbacks
     */
    public void demonstrateAsynchronousProduction() {
        logger.info("=== Demonstrating Asynchronous Production ===");
        
        for (int i = 0; i < 10; i++) {
            UserEvent event = createUserEvent((long) (i + 1), EventType.LOGIN);
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                USER_EVENTS_TOPIC, event.getEventId().toString(), event);
            
            // Asynchronous send with callback
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send record: {}", record.key(), exception);
                } else {
                    logger.debug("Record sent async: key={}, partition={}, offset={}", 
                        record.key(), metadata.partition(), metadata.offset());
                }
            });
        }
        
        // Ensure all async sends complete
        producer.flush();
        logger.info("All asynchronous sends completed");
    }
    
    /**
     * Demonstrates batch production for high throughput
     */
    public void demonstrateBatchProduction() {
        logger.info("=== Demonstrating Batch Production ===");
        
        long startTime = System.currentTimeMillis();
        int batchSize = 1000;
        
        for (int i = 0; i < batchSize; i++) {
            UserEvent event = createUserEvent((long) i, getRandomEventType());
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                USER_EVENTS_TOPIC, event.getEventId().toString(), event);
            
            producer.send(record);
            
            // Demonstrate batching by not flushing immediately
            if (i % 100 == 0) {
                producer.flush(); // Flush every 100 records
            }
        }
        
        producer.flush(); // Final flush
        long endTime = System.currentTimeMillis();
        
        logger.info("Batch production completed: {} records in {} ms ({} records/sec)",
            batchSize, endTime - startTime, batchSize * 1000.0 / (endTime - startTime));
    }
    
    /**
     * Demonstrates advanced partitioning strategies
     */
    public void demonstrateAdvancedPartitioning() {
        logger.info("=== Advanced Partitioning Demonstration ===");
        
        try {
            producer.beginTransaction();
            
            // 1. Geographic Partitioning
            demonstrateGeographicPartitioning();
            
            // 2. Temporal Partitioning 
            demonstrateTemporalPartitioning();
            
            // 3. User Tier Partitioning
            demonstrateUserTierPartitioning();
            
            // 4. Modulus Partitioning
            demonstrateModulusPartitioning();
            
            // 5. Consistent Hash Partitioning
            demonstrateConsistentHashPartitioning();
            
            producer.commitTransaction();
            logger.info("Advanced partitioning demonstration completed successfully");
            
        } catch (Exception e) {
            logger.error("Error in partitioning demonstration", e);
            producer.abortTransaction();
        }
    }
    
    /**
     * Geographic partitioning for global applications
     */
    private void demonstrateGeographicPartitioning() {
        logger.info("--- Geographic Partitioning ---");
        
        String[] regions = {"US", "EU", "APAC"};
        String[] countries = {"USA", "DEU", "JPN"};
        
        for (int i = 0; i < 9; i++) {
            String region = regions[i % regions.length];
            String country = countries[i % countries.length];
            Long userId = 1000L + i;
            
            // Geographic key: REGION:COUNTRY:USER_ID
            String key = String.format("%s:%s:%d", region, country, userId);
            
            UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(EventType.LOGIN)
                .setTimestamp(Instant.now().toEpochMilli())
                .setRegion(region)
                .setCountry(country)
                .build();
            
            sendWithCallback(USER_EVENTS_TOPIC, key, event, "Geographic");
        }
    }
    
    /**
     * Temporal partitioning for time-series data
     */
    private void demonstrateTemporalPartitioning() {
        logger.info("--- Temporal Partitioning ---");
        
        long baseTime = System.currentTimeMillis();
        
        for (int i = 0; i < 12; i++) {
            // Create events at different time intervals (hourly)
            long timestamp = baseTime + (i * 3600000L); // 1 hour intervals
            Long userId = 2000L + (i % 4); // Cycle through 4 users
            
            UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(EventType.PAGE_VIEW)
                .setTimestamp(timestamp)
                .setRegion("US")
                .setCountry("USA")
                .build();
            
            // Temporal key: HOUR_BUCKET:USER_ID
            long hourBucket = timestamp / 3600000L; // Hour bucket
            String key = String.format("%d:%d", hourBucket, userId);
            
            sendWithCallback(USER_EVENTS_TOPIC, key, event, "Temporal");
        }
    }
    
    /**
     * User tier partitioning for SLA differentiation
     */
    private void demonstrateUserTierPartitioning() {
        logger.info("--- User Tier Partitioning ---");
        
        String[] tiers = {"VIP", "PREMIUM", "STANDARD"};
        
        for (int i = 0; i < 12; i++) {
            String tier = tiers[i % tiers.length];
            Long userId = 3000L + i;
            
            UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(EventType.PURCHASE)
                .setTimestamp(Instant.now().toEpochMilli())
                .setRegion("US")
                .setCountry("USA")
                .build();
            
            // Tier-based key: TIER:USER_ID
            String key = String.format("%s:%d", tier, userId);
            
            sendWithCallback(USER_EVENTS_TOPIC, key, event, "UserTier");
        }
    }
    
    /**
     * Consistent hash partitioning for session affinity
     */
    private void demonstrateConsistentHashPartitioning() {
        logger.info("--- Consistent Hash Partitioning ---");
        
        String[] sessionIds = {"session_abc123", "session_def456", "session_ghi789"};
        
        // Send multiple events per session to demonstrate consistency
        for (int round = 0; round < 3; round++) {
            for (String sessionId : sessionIds) {
                Long userId = 4000L + Math.abs(sessionId.hashCode() % 1000);
                
                UserEvent event = UserEvent.newBuilder()
                    .setUserId(userId)
                    .setEventType(EventType.PAGE_VIEW)
                    .setTimestamp(Instant.now().toEpochMilli())
                    .setRegion("US")
                    .setCountry("USA")
                    .setSessionId(sessionId)
                    .build();
                
                // Use session ID as key for consistent hashing
                sendWithCallback(USER_EVENTS_TOPIC, sessionId, event, "ConsistentHash");
                
                // Small delay to show round progression
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    /**
     * Modulus partitioning for even distribution with numeric keys
     */
    private void demonstrateModulusPartitioning() {
        logger.info("--- Modulus Partitioning ---");
        
        // Scenario 1: Sequential user IDs for even distribution
        for (int i = 1; i <= 12; i++) {
            Long userId = 5000L + i;
            
            UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(EventType.LOGIN)
                .setTimestamp(Instant.now().toEpochMilli())
                .setRegion("US")
                .setCountry("USA")
                .setSessionId("session_" + userId)
                .build();
            
            // Use user ID as key for modulus partitioning
            sendWithCallback(USER_EVENTS_TOPIC, userId.toString(), event, "Modulus");
        }
        
        // Scenario 2: Order IDs with predictable distribution
        for (int i = 1; i <= 10; i++) {
            Long orderId = 10000L + i;
            Long userId = 6000L + (i % 5); // Cycle through 5 users
            
            UserEvent event = UserEvent.newBuilder()
                .setUserId(userId)
                .setEventType(EventType.PURCHASE)
                .setTimestamp(Instant.now().toEpochMilli())
                .setRegion("US")
                .setCountry("USA")
                .setSessionId("order_session_" + orderId)
                .build();
            
            // Use order ID for deterministic partition assignment
            sendWithCallback(USER_EVENTS_TOPIC, "order:" + orderId, event, "Modulus");
        }
    }
    
    /**
     * Helper method to send messages with detailed callback logging
     */
    private void sendWithCallback(String topic, String key, Object value, String strategy) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, value);
        
        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                logger.info("{} partitioning - Key: {}, Partition: {}, Offset: {}", 
                    strategy, key, metadata.partition(), metadata.offset());
            } else {
                logger.error("{} partitioning failed for key: {}", strategy, key, exception);
            }
        });
    }
    
    /**
     * Creates a sample user with realistic data
     */
    private User createSampleUser() {
        Map<String, String> preferences = new HashMap<>();
        preferences.put("theme", "dark");
        preferences.put("notifications", "enabled");
        preferences.put("language", "en");
        
        return User.newBuilder()
            .setId(random.nextLong())
            .setEmail("user" + random.nextInt(10000) + "@example.com")
            .setFirstName("John")
            .setLastName("Doe")
            .setCreatedAt(Instant.now().toEpochMilli())
            .setIsActive(true)
            .setPreferences(preferences)
            .build();
    }
    
    /**
     * Creates a sample user event
     */
    private UserEvent createUserEvent(Long userId, EventType eventType) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("ip", "192.168.1." + random.nextInt(255));
        metadata.put("userAgent", "Mozilla/5.0 (compatible; KafkaShowcase/1.0)");
        
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(userId)
            .setEventType(eventType)
            .setTimestamp(Instant.now().toEpochMilli())
            .setMetadata(metadata)
            .setSessionId(UUID.randomUUID().toString())
            .build();
    }
    
    /**
     * Returns a random event type for demonstration
     */
    private EventType getRandomEventType() {
        EventType[] types = EventType.values();
        return types[random.nextInt(types.length)];
    }
    
    /**
     * Clean up resources
     */
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Producer closed");
        }
    }
}
