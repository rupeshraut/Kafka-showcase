package com.example.kafka.partitioning;

import com.example.kafka.config.KafkaConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Comprehensive demonstration of advanced partitioning strategies and consumer assignment patterns
 * for various real-world use cases.
 * 
 * This demo showcases:
 * 1. Geographic-based partitioning and assignment
 * 2. Load-balanced assignment with capacity awareness
 * 3. Temporal partitioning for time-series data
 * 4. Priority-based consumer assignment
 * 5. Cross-zone resilient assignment for fault tolerance
 */
public class PartitioningPatternsDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(PartitioningPatternsDemo.class);
    
    private static final String TOPIC_GLOBAL_EVENTS = "global-user-events";
    private static final String TOPIC_FINANCIAL_DATA = "financial-transactions";
    private static final String TOPIC_IOT_METRICS = "iot-sensor-data";
    private static final String TOPIC_AUDIT_LOGS = "audit-events";
    
    /**
     * Demonstrates various real-world partitioning and assignment patterns
     */
    public static void main(String[] args) {
        logger.info("Starting Advanced Partitioning Patterns Demo");
        
        try {
            // 1. Geographic Partitioning Demo
            demonstrateGeographicPartitioning();
            
            // 2. Load-Balanced Assignment Demo
            demonstrateLoadBalancedAssignment();
            
            // 3. Temporal Partitioning Demo
            demonstrateTemporalPartitioning();
            
            // 4. Priority-Based Assignment Demo
            demonstratePriorityBasedAssignment();
            
            // 5. Cross-Zone Resilient Assignment Demo
            demonstrateCrossZoneResilientAssignment();
            
            // 6. Modulus Partitioning Demo
            demonstrateModulusPartitioning();
            
            // 7. Custom Partitioning Strategies Demo
            demonstrateCustomPartitioningStrategies();
            
        } catch (Exception e) {
            logger.error("Error in partitioning patterns demo", e);
        }
        
        logger.info("Advanced Partitioning Patterns Demo completed");
    }
    
    /**
     * Real-world use case: Global e-commerce platform
     * 
     * Scenario: Users from different regions (US, EU, APAC) generate events.
     * Requirements: Events should be processed by consumers in the same region
     * for latency reduction and data residency compliance.
     */
    private static void demonstrateGeographicPartitioning() {
        logger.info("=== Geographic Partitioning Demo ===");
        
        // Create producer with geographic partitioner
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Simulate users from different regions
            String[] regions = {"US", "EU", "APAC"};
            String[] userIds = {"user123", "user456", "user789"};
            
            for (int i = 0; i < 15; i++) {
                String region = regions[i % regions.length];
                String userId = userIds[i % userIds.length];
                String eventData = String.format("{\"userId\":\"%s\", \"region\":\"%s\", \"action\":\"page_view\", \"timestamp\":%d}", 
                    userId, region, System.currentTimeMillis());
                
                // Key includes region for geographic partitioning
                String key = String.format("%s:%s", region, userId);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, key, eventData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Geographic event sent - Region: {}, Partition: {}, Offset: {}", 
                            region, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send geographic event", exception);
                    }
                });
            }
            
            producer.flush();
        }
        
        // Create consumers with geographic affinity assignment
        createGeographicConsumers();
    }
    
    /**
     * Creates consumers with geographic affinity assignment strategy
     */
    private static void createGeographicConsumers() {
        String[] regions = {"US", "EU", "APAC"};
        List<Thread> consumerThreads = new ArrayList<>();
        
        for (String region : regions) {
            Thread consumerThread = new Thread(() -> {
                Properties consumerProps = KafkaConfigFactory.createConsumerConfig();
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "geographic-consumer-group");
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + region);
                consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, AdvancedPartitionAssignor.class.getName());
                
                // Add region metadata to consumer
                String userData = String.format("strategy=GEOGRAPHIC_AFFINITY;region=%s", region);
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-" + region);
                
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    // Set user data for assignment strategy
                    // Note: In a real implementation, this would be set through a custom ConsumerRebalanceListener
                    
                    consumer.subscribe(Collections.singletonList(TOPIC_GLOBAL_EVENTS));
                    
                    for (int i = 0; i < 5; i++) { // Read a few messages for demo
                        var records = consumer.poll(Duration.ofMillis(1000));
                        records.forEach(record -> {
                            logger.info("Geographic consumer {} processed: Partition={}, Key={}, Value={}", 
                                region, record.partition(), record.key(), record.value());
                        });
                    }
                }
            });
            
            consumerThread.start();
            consumerThreads.add(consumerThread);
        }
        
        // Wait for consumers to process some messages
        consumerThreads.forEach(thread -> {
            try {
                thread.join(5000); // Wait up to 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Real-world use case: High-frequency trading system
     * 
     * Scenario: Trading events have varying processing complexity.
     * Some consumers have more powerful hardware than others.
     * Requirements: Distribute load based on consumer capacity.
     */
    private static void demonstrateLoadBalancedAssignment() {
        logger.info("=== Load-Balanced Assignment Demo ===");
        
        // Create consumers with different capacities
        createCapacityAwareConsumers();
    }
    
    /**
     * Creates consumers with different processing capacities
     */
    private static void createCapacityAwareConsumers() {
        // Simulate consumers with different capacities (CPU cores, memory, etc.)
        Map<String, Double> consumerCapacities = Map.of(
            "high-capacity-consumer", 4.0,    // 4x baseline capacity
            "medium-capacity-consumer", 2.0,   // 2x baseline capacity
            "low-capacity-consumer", 1.0       // 1x baseline capacity
        );
        
        List<Thread> consumerThreads = new ArrayList<>();
        
        for (Map.Entry<String, Double> entry : consumerCapacities.entrySet()) {
            String consumerId = entry.getKey();
            Double capacity = entry.getValue();
            
            Thread consumerThread = new Thread(() -> {
                Properties consumerProps = KafkaConfigFactory.createConsumerConfig();
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "capacity-aware-group");
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
                consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, AdvancedPartitionAssignor.class.getName());
                
                // Add capacity metadata
                String userData = String.format("strategy=CAPACITY_AWARE;capacity=%.1f", capacity);
                
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    consumer.subscribe(Collections.singletonList(TOPIC_FINANCIAL_DATA));
                    
                    logger.info("Capacity-aware consumer {} (capacity: {}) started", consumerId, capacity);
                    
                    for (int i = 0; i < 3; i++) {
                        var records = consumer.poll(Duration.ofMillis(1000));
                        records.forEach(record -> {
                            logger.info("Capacity consumer {} processed: Partition={}", 
                                consumerId, record.partition());
                        });
                    }
                }
            });
            
            consumerThread.start();
            consumerThreads.add(consumerThread);
        }
        
        // Wait for consumers
        consumerThreads.forEach(thread -> {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Real-world use case: IoT time-series data processing
     * 
     * Scenario: Sensor data needs to be partitioned by time windows
     * for efficient time-range queries and data retention.
     * Requirements: Partition by hour/day for time-based processing.
     */
    private static void demonstrateTemporalPartitioning() {
        logger.info("=== Temporal Partitioning Demo ===");
        
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Simulate IoT sensor data over different time periods
            long baseTime = System.currentTimeMillis();
            String[] sensorIds = {"sensor001", "sensor002", "sensor003"};
            
            for (int i = 0; i < 20; i++) {
                String sensorId = sensorIds[i % sensorIds.length];
                long timestamp = baseTime + (i * 3600000L); // 1 hour intervals
                
                String sensorData = String.format(
                    "{\"sensorId\":\"%s\", \"temperature\":%.1f, \"humidity\":%.1f, \"timestamp\":%d}",
                    sensorId, 20.0 + (i % 10), 50.0 + (i % 20), timestamp
                );
                
                // Key includes timestamp for temporal partitioning
                String key = String.format("%s:%d", sensorId, timestamp);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_IOT_METRICS, key, sensorData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Temporal event sent - Sensor: {}, Partition: {}, Offset: {}", 
                            sensorId, metadata.partition(), metadata.offset());
                    }
                });
            }
            
            producer.flush();
        }
    }
    
    /**
     * Real-world use case: Critical system monitoring
     * 
     * Scenario: Different types of alerts have different priorities.
     * High-priority consumers should get critical alerts first.
     * Requirements: Priority-based assignment with sticky behavior.
     */
    private static void demonstratePriorityBasedAssignment() {
        logger.info("=== Priority-Based Assignment Demo ===");
        
        // Create consumers with different priorities
        Map<String, Integer> consumerPriorities = Map.of(
            "critical-alert-consumer", 10,     // Highest priority
            "warning-alert-consumer", 5,       // Medium priority
            "info-alert-consumer", 1           // Lowest priority
        );
        
        List<Thread> consumerThreads = new ArrayList<>();
        
        for (Map.Entry<String, Integer> entry : consumerPriorities.entrySet()) {
            String consumerId = entry.getKey();
            Integer priority = entry.getValue();
            
            Thread consumerThread = new Thread(() -> {
                Properties consumerProps = KafkaConfigFactory.createConsumerConfig();
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "priority-alert-group");
                consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
                consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, AdvancedPartitionAssignor.class.getName());
                
                // Add priority metadata
                String userData = String.format("strategy=STICKY_PRIORITY;priority=%d", priority);
                
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    consumer.subscribe(Collections.singletonList(TOPIC_AUDIT_LOGS));
                    
                    logger.info("Priority consumer {} (priority: {}) started", consumerId, priority);
                    
                    for (int i = 0; i < 3; i++) {
                        var records = consumer.poll(Duration.ofMillis(1000));
                        records.forEach(record -> {
                            logger.info("Priority consumer {} processed: Partition={}", 
                                consumerId, record.partition());
                        });
                    }
                }
            });
            
            consumerThread.start();
            consumerThreads.add(consumerThread);
        }
        
        // Wait for consumers
        consumerThreads.forEach(thread -> {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Real-world use case: Multi-region disaster recovery
     * 
     * Scenario: Consumers are distributed across availability zones.
     * Requirements: Ensure fault tolerance by distributing partitions
     * across zones so that zone failure doesn't affect all consumers.
     */
    private static void demonstrateCrossZoneResilientAssignment() {
        logger.info("=== Cross-Zone Resilient Assignment Demo ===");
        
        // Create consumers in different zones
        String[] zones = {"zone-a", "zone-b", "zone-c"};
        List<Thread> consumerThreads = new ArrayList<>();
        
        for (String zone : zones) {
            for (int i = 0; i < 2; i++) { // 2 consumers per zone
                String consumerId = String.format("consumer-%s-%d", zone, i);
                
                Thread consumerThread = new Thread(() -> {
                    Properties consumerProps = KafkaConfigFactory.createConsumerConfig();
                    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "cross-zone-resilient-group");
                    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
                    consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, AdvancedPartitionAssignor.class.getName());
                    
                    // Add zone metadata
                    String userData = String.format("strategy=CROSS_ZONE_RESILIENT;zone=%s", zone);
                    
                    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                        consumer.subscribe(Collections.singletonList(TOPIC_GLOBAL_EVENTS));
                        
                        logger.info("Cross-zone consumer {} (zone: {}) started", consumerId, zone);
                        
                        for (int j = 0; j < 2; j++) {
                            var records = consumer.poll(Duration.ofMillis(1000));
                            records.forEach(record -> {
                                logger.info("Zone {} consumer {} processed: Partition={}", 
                                    zone, consumerId, record.partition());
                            });
                        }
                    }
                });
                
                consumerThread.start();
                consumerThreads.add(consumerThread);
            }
        }
        
        // Wait for consumers
        consumerThreads.forEach(thread -> {
            try {
                thread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
    
    /**
     * Real-world use case: Order processing system with sequential order IDs
     * 
     * Scenario: E-commerce platform generates orders with sequential IDs (1, 2, 3, ...).
     * Requirements: Simple, predictable partition assignment with even distribution.
     * Modulus partitioning ensures deterministic assignment and even load distribution.
     */
    private static void demonstrateModulusPartitioning() {
        logger.info("=== Modulus Partitioning Demo ===");
        
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        producerProps.put("partitioning.strategy", "MODULUS");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            // Scenario 1: Sequential order IDs
            logger.info("--- Sequential Order IDs ---");
            for (int orderId = 1001; orderId <= 1020; orderId++) {
                String orderData = String.format(
                    "{\"orderId\":%d, \"customerId\":\"%s\", \"amount\":%.2f, \"timestamp\":%d}",
                    orderId, "customer" + (orderId % 5), 99.99 + (orderId % 100), System.currentTimeMillis()
                );
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, 
                    String.valueOf(orderId), orderData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sequential order {} -> partition {} (modulus: {} % partitions)", 
                            record.key(), metadata.partition(), record.key());
                    }
                });
            }
            
            // Scenario 2: User IDs with numeric extraction
            logger.info("--- User IDs with Mixed Format ---");
            String[] userKeys = {"user:2001", "customer-3001", "account_4001", "5001", "member:6001"};
            for (String userKey : userKeys) {
                String userData = String.format(
                    "{\"userId\":\"%s\", \"action\":\"login\", \"timestamp\":%d}",
                    userKey, System.currentTimeMillis()
                );
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, 
                    userKey, userData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("User key {} -> partition {} (numeric extraction + modulus)", 
                            record.key(), metadata.partition());
                    }
                });
            }
            
            // Scenario 3: Transaction IDs for financial processing
            logger.info("--- Financial Transaction IDs ---");
            for (int i = 0; i < 15; i++) {
                long transactionId = 7000000L + i;
                String transactionData = String.format(
                    "{\"transactionId\":%d, \"amount\":%.2f, \"currency\":\"USD\", \"timestamp\":%d}",
                    transactionId, 250.0 + (i * 10), System.currentTimeMillis()
                );
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_FINANCIAL_DATA, 
                    String.valueOf(transactionId), transactionData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Transaction {} -> partition {} (large numeric modulus)", 
                            record.key(), metadata.partition());
                    }
                });
            }
            
            producer.flush();
        }
        
        logger.info("Modulus partitioning benefits:");
        logger.info("✓ Deterministic: Same key always goes to same partition");
        logger.info("✓ Even distribution: Numeric keys distribute evenly across partitions");
        logger.info("✓ Simple and fast: Minimal computational overhead");
        logger.info("✓ Predictable: Easy to understand and debug partition assignment");
    }
    
    /**
     * Demonstrates custom partitioning strategies for specific use cases
     */
    private static void demonstrateCustomPartitioningStrategies() {
        logger.info("=== Custom Partitioning Strategies Demo ===");
        
        // Demonstrate different partitioning strategies
        demonstrateUserTierPartitioning();
        demonstrateConsistentHashPartitioning();
        demonstrateHybridPartitioning();
    }
    
    /**
     * User tier partitioning: VIP users get dedicated partitions
     */
    private static void demonstrateUserTierPartitioning() {
        logger.info("--- User Tier Partitioning ---");
        
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            String[] userTiers = {"VIP", "PREMIUM", "STANDARD"};
            
            for (int i = 0; i < 12; i++) {
                String userTier = userTiers[i % userTiers.length];
                String userId = "user" + (1000 + i);
                
                String eventData = String.format(
                    "{\"userId\":\"%s\", \"tier\":\"%s\", \"action\":\"purchase\", \"amount\":%.2f}",
                    userId, userTier, 100.0 + (i * 10)
                );
                
                // Key includes tier for tier-based partitioning
                String key = String.format("%s:%s", userTier, userId);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, key, eventData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("User tier event sent - Tier: {}, Partition: {}", 
                            userTier, metadata.partition());
                    }
                });
            }
            
            producer.flush();
        }
    }
    
    /**
     * Consistent hash partitioning: Ensures same key always goes to same partition
     */
    private static void demonstrateConsistentHashPartitioning() {
        logger.info("--- Consistent Hash Partitioning ---");
        
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            String[] sessionIds = {"session_abc123", "session_def456", "session_ghi789"};
            
            // Send multiple events for same sessions to demonstrate consistency
            for (int round = 0; round < 3; round++) {
                for (String sessionId : sessionIds) {
                    String eventData = String.format(
                        "{\"sessionId\":\"%s\", \"event\":\"page_view\", \"round\":%d, \"timestamp\":%d}",
                        sessionId, round, System.currentTimeMillis()
                    );
                    
                    // Use session ID as key for consistent hashing
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, sessionId, eventData);
                    
                    producer.send(record, (metadata, exception) -> {
                        if (exception == null) {
                            logger.info("Consistent hash event sent - Session: {}, Partition: {} (should be consistent)", 
                                sessionId, metadata.partition());
                        }
                    });
                }
            }
            
            producer.flush();
        }
    }
    
    /**
     * Hybrid partitioning: Combines multiple strategies based on message content
     */
    private static void demonstrateHybridPartitioning() {
        logger.info("--- Hybrid Partitioning ---");
        
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            // Mix different types of events that should use different partitioning strategies
            String[] eventTypes = {"FINANCIAL_TRANSACTION", "USER_INTERACTION", "SYSTEM_METRIC"};
            String[] regions = {"US", "EU", "APAC"};
            
            for (int i = 0; i < 15; i++) {
                String eventType = eventTypes[i % eventTypes.length];
                String region = regions[i % regions.length];
                String entityId = "entity" + (i % 5);
                
                String eventData = String.format(
                    "{\"type\":\"%s\", \"region\":\"%s\", \"entityId\":\"%s\", \"value\":%d}",
                    eventType, region, entityId, i * 100
                );
                
                // Create hybrid key that includes multiple partitioning factors
                String key = String.format("%s:%s:%s", eventType, region, entityId);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_GLOBAL_EVENTS, key, eventData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Hybrid event sent - Type: {}, Region: {}, Partition: {}", 
                            eventType, region, metadata.partition());
                    }
                });
            }
            
            producer.flush();
        }
    }
}
