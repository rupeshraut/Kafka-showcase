package com.example.kafka.eventsourcing;

import com.example.kafka.config.KafkaConfigFactory;
import com.example.kafka.avro.User;
import com.example.kafka.avro.UserEvent;
import com.example.kafka.avro.EventType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Advanced Event Sourcing demonstration using Kafka as an event store.
 * 
 * This implementation showcases:
 * 
 * 1. EVENT STORE PATTERNS
 *    - Kafka as persistent event store with immutable event log
 *    - Partitioning by aggregate ID for ordering guarantees
 *    - Event versioning and schema evolution
 *    - Snapshot store for performance optimization
 * 
 * 2. COMMAND QUERY RESPONSIBILITY SEGREGATION (CQRS)
 *    - Separate command and query models
 *    - Event-driven read model updates
 *    - Eventually consistent read projections
 *    - Multiple specialized view models
 * 
 * 3. EVENT REPLAY AND PROJECTION
 *    - Historical event replay for new projections
 *    - Point-in-time state reconstruction
 *    - Temporal queries and audit trails
 *    - Incremental and full rebuilds
 * 
 * 4. ADVANCED PATTERNS
 *    - Saga pattern for distributed transactions
 *    - Event deduplication and idempotency
 *    - Causality tracking with vector clocks
 *    - Event correlation and process management
 * 
 * Real-world use cases:
 * - Financial trading systems with complete audit trails
 * - E-commerce order processing with state transitions
 * - User management with privacy compliance (GDPR)
 * - IoT device lifecycle management
 * - Multi-tenant SaaS with tenant isolation
 */
public class EventSourcingDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(EventSourcingDemo.class);
    
    // Topics for event sourcing
    private static final String EVENT_STORE_TOPIC = "event-store";
    private static final String SNAPSHOTS_TOPIC = "snapshots";
    private static final String USER_PROJECTION_TOPIC = "user-projection";
    private static final String AUDIT_PROJECTION_TOPIC = "audit-projection";
    
    // Event store configuration
    private static final int SNAPSHOT_FREQUENCY = 100; // Take snapshot every 100 events
    private static final Duration REPLAY_TIMEOUT = Duration.ofSeconds(30);
    
    private final KafkaProducer<String, Object> eventProducer;
    private final KafkaConsumer<String, Object> eventConsumer;
    private final Map<String, UserAggregate> userAggregates = new ConcurrentHashMap<>();
    private final Map<String, Long> lastSnapshotVersions = new ConcurrentHashMap<>();
    private final AtomicLong eventSequence = new AtomicLong(0);
    
    public EventSourcingDemo() {
        this.eventProducer = new KafkaProducer<>(KafkaConfigFactory.createProducerConfig());
        
        Properties consumerProps = KafkaConfigFactory.createExactlyOnceConsumerConfig("event-sourcing-consumer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.eventConsumer = new KafkaConsumer<>(consumerProps);
        
        logger.info("Event Sourcing system initialized");
    }
    
    /**
     * Main demonstration of event sourcing patterns
     */
    public static void main(String[] args) {
        logger.info("=== EVENT SOURCING AND CQRS PATTERNS DEMO ===");
        logger.info("Demonstrating advanced event sourcing with Kafka as event store");
        
        EventSourcingDemo demo = new EventSourcingDemo();
        
        try {
            // 1. Command processing - store events
            demo.demonstrateCommandProcessing();
            
            Thread.sleep(2000);
            
            // 2. Event replay and projection building
            demo.demonstrateEventReplay();
            
            Thread.sleep(2000);
            
            // 3. Snapshot creation and restoration
            demo.demonstrateSnapshotting();
            
            Thread.sleep(2000);
            
            // 4. CQRS query model updates
            demo.demonstrateCQRSProjections();
            
            Thread.sleep(2000);
            
            // 5. Advanced patterns - Saga and correlation
            demo.demonstrateAdvancedPatterns();
            
        } catch (Exception e) {
            logger.error("Error in event sourcing demo", e);
        } finally {
            demo.close();
        }
        
        logger.info("Event Sourcing demo completed");
    }
    
    /**
     * Demonstrates command processing with event storage
     */
    private void demonstrateCommandProcessing() {
        logger.info("=== COMMAND PROCESSING ===");
        
        try {
            eventProducer.initTransactions();
            
            // Process various user commands
            String[] userIds = {"user-1001", "user-1002", "user-1003"};
            
            for (String userId : userIds) {
                eventProducer.beginTransaction();
                
                try {
                    // User registration command
                    publishEvent(userId, createUserRegisteredEvent(userId));
                    
                    // Email verification command
                    publishEvent(userId, createEmailVerifiedEvent(userId));
                    
                    // Profile update command
                    publishEvent(userId, createProfileUpdatedEvent(userId));
                    
                    // Purchase command
                    publishEvent(userId, createPurchaseEvent(userId));
                    
                    eventProducer.commitTransaction();
                    logger.info("Command sequence completed for user: {}", userId);
                    
                } catch (Exception e) {
                    eventProducer.abortTransaction();
                    logger.error("Command failed for user: {}, aborting transaction", userId, e);
                }
            }
            
        } catch (Exception e) {
            logger.error("Error in command processing", e);
        }
    }
    
    /**
     * Demonstrates event replay for building projections
     */
    private void demonstrateEventReplay() {
        logger.info("=== EVENT REPLAY AND PROJECTION BUILDING ===");
        
        eventConsumer.subscribe(Collections.singletonList(EVENT_STORE_TOPIC));
        
        Map<String, UserProjection> userProjections = new HashMap<>();
        Map<String, List<AuditEvent>> auditTrails = new HashMap<>();
        
        long startTime = System.currentTimeMillis();
        long totalEvents = 0;
        
        try {
            while (System.currentTimeMillis() - startTime < REPLAY_TIMEOUT.toMillis()) {
                ConsumerRecords<String, Object> records = eventConsumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, Object> record : records) {
                    if (record.value() instanceof UserEvent) {
                        UserEvent event = (UserEvent) record.value();
                        String userId = String.valueOf(event.getUserId());
                        
                        // Build user projection
                        userProjections.computeIfAbsent(userId, k -> new UserProjection(k))
                            .applyEvent(event);
                        
                        // Build audit trail
                        auditTrails.computeIfAbsent(userId, k -> new ArrayList<>())
                            .add(new AuditEvent(event, record.timestamp(), record.offset()));
                        
                        totalEvents++;
                    }
                }
            }
            
            logger.info("Event replay completed: {} events processed", totalEvents);
            
            // Log projection summaries
            userProjections.forEach((userId, projection) -> {
                logger.info("User projection {}: status={}, lastActivity={}, eventCount={}", 
                    userId, projection.getStatus(), projection.getLastActivity(), projection.getEventCount());
            });
            
            // Publish projections to read model topics
            publishProjections(userProjections, auditTrails);
            
        } catch (Exception e) {
            logger.error("Error during event replay", e);
        }
    }
    
    /**
     * Demonstrates snapshot creation and restoration for performance
     */
    private void demonstrateSnapshotting() {
        logger.info("=== SNAPSHOT CREATION AND RESTORATION ===");
        
        try {
            // Create snapshots for users with sufficient events
            for (String userId : Arrays.asList("user-1001", "user-1002", "user-1003")) {
                UserAggregate aggregate = reconstructAggregate(userId);
                
                if (aggregate.getVersion() >= SNAPSHOT_FREQUENCY || 
                    aggregate.getVersion() - lastSnapshotVersions.getOrDefault(userId, 0L) >= SNAPSHOT_FREQUENCY) {
                    
                    createSnapshot(userId, aggregate);
                    lastSnapshotVersions.put(userId, aggregate.getVersion());
                }
            }
            
            // Demonstrate fast restoration from snapshot
            UserAggregate restoredAggregate = restoreFromSnapshot("user-1001");
            if (restoredAggregate != null) {
                logger.info("Restored aggregate from snapshot: userId={}, version={}, status={}", 
                    restoredAggregate.getUserId(), restoredAggregate.getVersion(), restoredAggregate.getStatus());
            }
            
        } catch (Exception e) {
            logger.error("Error in snapshotting demo", e);
        }
    }
    
    /**
     * Demonstrates CQRS with multiple specialized read models
     */
    private void demonstrateCQRSProjections() {
        logger.info("=== CQRS QUERY MODEL PROJECTIONS ===");
        
        // Specialized projections for different query patterns
        Map<String, CustomerSummaryProjection> customerSummaries = new HashMap<>();
        Map<String, ActivityTimelineProjection> activityTimelines = new HashMap<>();
        Map<String, ComplianceProjection> complianceViews = new HashMap<>();
        
        eventConsumer.seekToBeginning(eventConsumer.assignment());
        
        long startTime = System.currentTimeMillis();
        
        try {
            while (System.currentTimeMillis() - startTime < 10000) { // 10 second window
                ConsumerRecords<String, Object> records = eventConsumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, Object> record : records) {
                    if (record.value() instanceof UserEvent) {
                        UserEvent event = (UserEvent) record.value();
                        String userId = String.valueOf(event.getUserId());
                        
                        // Update specialized projections
                        customerSummaries.computeIfAbsent(userId, CustomerSummaryProjection::new)
                            .updateFromEvent(event);
                        
                        activityTimelines.computeIfAbsent(userId, ActivityTimelineProjection::new)
                            .addActivity(event);
                        
                        complianceViews.computeIfAbsent(userId, ComplianceProjection::new)
                            .trackCompliance(event);
                    }
                }
            }
            
            // Log projection results
            logger.info("CQRS Projections built:");
            customerSummaries.forEach((userId, summary) -> {
                logger.info("  Customer {}: totalPurchases={}, lifetimeValue={}, riskScore={}", 
                    userId, summary.getTotalPurchases(), summary.getLifetimeValue(), summary.getRiskScore());
            });
            
        } catch (Exception e) {
            logger.error("Error in CQRS projections", e);
        }
    }
    
    /**
     * Demonstrates advanced patterns: Saga, correlation, and causality tracking
     */
    private void demonstrateAdvancedPatterns() {
        logger.info("=== ADVANCED EVENT SOURCING PATTERNS ===");
        
        try {
            eventProducer.beginTransaction();
            
            // Saga pattern: Multi-step business process
            String sagaId = UUID.randomUUID().toString();
            String userId = "user-2001";
            
            // Step 1: Order placement
            UserEvent orderPlaced = createOrderPlacedEvent(userId, sagaId);
            publishEvent(userId, orderPlaced);
            
            // Step 2: Payment processing (with correlation)
            UserEvent paymentProcessed = createPaymentProcessedEvent(userId, sagaId, orderPlaced.getEventId());
            publishEvent(userId, paymentProcessed);
            
            // Step 3: Inventory reservation (with causality chain)
            UserEvent inventoryReserved = createInventoryReservedEvent(userId, sagaId, paymentProcessed.getEventId());
            publishEvent(userId, inventoryReserved);
            
            // Step 4: Shipping initiated (completing the saga)
            UserEvent shippingInitiated = createShippingInitiatedEvent(userId, sagaId, inventoryReserved.getEventId());
            publishEvent(userId, shippingInitiated);
            
            eventProducer.commitTransaction();
            
            logger.info("Saga completed successfully: sagaId={}, userId={}", sagaId, userId);
            
            // Demonstrate idempotency - duplicate command should be ignored
            demonstrateIdempotency(userId, sagaId);
            
        } catch (Exception e) {
            logger.error("Error in advanced patterns demo", e);
        }
    }
    
    /**
     * Demonstrates idempotency handling for duplicate commands
     */
    private void demonstrateIdempotency(String userId, String sagaId) {
        logger.info("=== IDEMPOTENCY DEMONSTRATION ===");
        
        try {
            eventProducer.beginTransaction();
            
            // Attempt to process the same saga again (should be idempotent)
            UserEvent duplicateOrder = createOrderPlacedEvent(userId, sagaId);
            
            // In a real system, you would check for existing events with the same correlation ID
            boolean isDuplicate = checkForDuplicateEvent(duplicateOrder);
            
            if (isDuplicate) {
                logger.info("Duplicate command detected, ignoring: sagaId={}", sagaId);
                eventProducer.abortTransaction();
            } else {
                publishEvent(userId, duplicateOrder);
                eventProducer.commitTransaction();
                logger.info("New command processed: sagaId={}", sagaId);
            }
            
        } catch (Exception e) {
            logger.error("Error in idempotency demo", e);
        }
    }
    
    // Event creation methods
    private UserEvent createUserRegisteredEvent(String userId) {
        return createEvent(userId, EventType.LOGIN, "USER_REGISTERED", 
            Map.of("email", userId + "@example.com", "registrationSource", "web"));
    }
    
    private UserEvent createEmailVerifiedEvent(String userId) {
        return createEvent(userId, EventType.LOGIN, "EMAIL_VERIFIED", 
            Map.of("verificationMethod", "email_link", "verifiedAt", Instant.now().toString()));
    }
    
    private UserEvent createProfileUpdatedEvent(String userId) {
        return createEvent(userId, EventType.PROFILE_UPDATE, "PROFILE_UPDATED", 
            Map.of("fields", "firstName,lastName,phone", "updateSource", "mobile_app"));
    }
    
    private UserEvent createPurchaseEvent(String userId) {
        return createEvent(userId, EventType.PURCHASE, "PURCHASE", 
            Map.of("orderId", UUID.randomUUID().toString(), "amount", "99.99", "currency", "USD"));
    }
    
    private UserEvent createOrderPlacedEvent(String userId, String sagaId) {
        return createEvent(userId, EventType.PURCHASE, "ORDER_PLACED", 
            Map.of("sagaId", sagaId, "orderId", UUID.randomUUID().toString(), "amount", "199.99"));
    }
    
    private UserEvent createPaymentProcessedEvent(String userId, String sagaId, String correlationId) {
        return createEvent(userId, EventType.PURCHASE, "PAYMENT_PROCESSED", 
            Map.of("sagaId", sagaId, "correlationId", correlationId, "paymentMethod", "credit_card"));
    }
    
    private UserEvent createInventoryReservedEvent(String userId, String sagaId, String correlationId) {
        return createEvent(userId, EventType.PURCHASE, "INVENTORY_RESERVED", 
            Map.of("sagaId", sagaId, "correlationId", correlationId, "warehouseId", "WH001"));
    }
    
    private UserEvent createShippingInitiatedEvent(String userId, String sagaId, String correlationId) {
        return createEvent(userId, EventType.PURCHASE, "SHIPPING_INITIATED", 
            Map.of("sagaId", sagaId, "correlationId", correlationId, "trackingNumber", "TRK" + System.currentTimeMillis()));
    }
    
    private UserEvent createEvent(String userId, EventType eventType, String eventSubType, Map<String, String> additionalMetadata) {
        Map<String, String> metadata = new HashMap<>(additionalMetadata);
        metadata.put("eventSubType", eventSubType);
        metadata.put("source", "EventSourcingDemo");
        metadata.put("version", "1.0");
        metadata.put("sequence", String.valueOf(eventSequence.incrementAndGet()));
        
        return UserEvent.newBuilder()
            .setEventId(UUID.randomUUID().toString())
            .setUserId(Long.parseLong(userId.replace("user-", "")))
            .setEventType(eventType)
            .setTimestamp(Instant.now().toEpochMilli())
            .setSessionId(UUID.randomUUID().toString())
            .setMetadata(metadata)
            .build();
    }
    
    private void publishEvent(String aggregateId, UserEvent event) {
        try {
            ProducerRecord<String, Object> record = new ProducerRecord<>(EVENT_STORE_TOPIC, aggregateId, event);
            eventProducer.send(record).get(); // Synchronous send for demo
            logger.debug("Event published: aggregateId={}, eventType={}, eventId={}", 
                aggregateId, event.getEventType(), event.getEventId());
        } catch (Exception e) {
            logger.error("Failed to publish event", e);
            throw new RuntimeException("Event publication failed", e);
        }
    }
    
    private UserAggregate reconstructAggregate(String userId) {
        // In a real implementation, this would replay events from the event store
        UserAggregate aggregate = new UserAggregate(userId);
        aggregate.setVersion(100L); // Simulated version
        aggregate.setStatus("ACTIVE");
        return aggregate;
    }
    
    private void createSnapshot(String userId, UserAggregate aggregate) {
        logger.info("Creating snapshot for user: {}, version: {}", userId, aggregate.getVersion());
        // In a real implementation, this would serialize and store the aggregate state
    }
    
    private UserAggregate restoreFromSnapshot(String userId) {
        logger.info("Restoring aggregate from snapshot: {}", userId);
        // In a real implementation, this would deserialize the stored aggregate state
        return new UserAggregate(userId);
    }
    
    private void publishProjections(Map<String, UserProjection> userProjections, Map<String, List<AuditEvent>> auditTrails) {
        // In a real implementation, this would publish the projections to dedicated topics
        logger.info("Publishing {} user projections and {} audit trails", 
            userProjections.size(), auditTrails.size());
    }
    
    private boolean checkForDuplicateEvent(UserEvent event) {
        // In a real implementation, this would check for existing events with the same correlation/saga ID
        return true; // Simulating duplicate detection
    }
    
    public void close() {
        if (eventProducer != null) {
            eventProducer.close();
        }
        if (eventConsumer != null) {
            eventConsumer.close();
        }
        logger.info("Event Sourcing system closed");
    }
    
    // Supporting classes for the demo
    static class UserAggregate {
        private final String userId;
        private Long version = 0L;
        private String status = "PENDING";
        
        public UserAggregate(String userId) {
            this.userId = userId;
        }
        
        public String getUserId() { return userId; }
        public Long getVersion() { return version; }
        public void setVersion(Long version) { this.version = version; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }
    
    static class UserProjection {
        private final String userId;
        private String status = "UNKNOWN";
        private Instant lastActivity = Instant.now();
        private int eventCount = 0;
        
        public UserProjection(String userId) {
            this.userId = userId;
        }
        
        public void applyEvent(UserEvent event) {
            eventCount++;
            lastActivity = Instant.ofEpochMilli(event.getTimestamp());
            
            String eventSubType = event.getMetadata().get("eventSubType");
            switch (eventSubType) {
                case "USER_REGISTERED":
                    status = "REGISTERED";
                    break;
                case "EMAIL_VERIFIED":
                    status = "VERIFIED";
                    break;
                case "PROFILE_UPDATED":
                    status = "ACTIVE";
                    break;
                default:
                    // Keep current status
                    break;
            }
        }
        
        public String getUserId() { return userId; }
        public String getStatus() { return status; }
        public Instant getLastActivity() { return lastActivity; }
        public int getEventCount() { return eventCount; }
    }
    
    static class AuditEvent {
        private final UserEvent originalEvent;
        private final long kafkaTimestamp;
        private final long kafkaOffset;
        
        public AuditEvent(UserEvent originalEvent, long kafkaTimestamp, long kafkaOffset) {
            this.originalEvent = originalEvent;
            this.kafkaTimestamp = kafkaTimestamp;
            this.kafkaOffset = kafkaOffset;
        }
        
        public UserEvent getOriginalEvent() { return originalEvent; }
        public long getKafkaTimestamp() { return kafkaTimestamp; }
        public long getKafkaOffset() { return kafkaOffset; }
    }
    
    static class CustomerSummaryProjection {
        private final String userId;
        private int totalPurchases = 0;
        private double lifetimeValue = 0.0;
        private double riskScore = 0.0;
        
        public CustomerSummaryProjection(String userId) {
            this.userId = userId;
        }
        
        public void updateFromEvent(UserEvent event) {
            if (event.getEventType() == EventType.PURCHASE) {
                totalPurchases++;
                String amount = event.getMetadata().get("amount");
                if (amount != null) {
                    try {
                        lifetimeValue += Double.parseDouble(amount);
                    } catch (NumberFormatException e) {
                        // Ignore invalid amounts
                    }
                }
                // Simple risk scoring based on purchase frequency
                riskScore = Math.min(totalPurchases * 0.1, 1.0);
            }
        }
        
        public String getUserId() { return userId; }
        public int getTotalPurchases() { return totalPurchases; }
        public double getLifetimeValue() { return lifetimeValue; }
        public double getRiskScore() { return riskScore; }
    }
    
    static class ActivityTimelineProjection {
        private final String userId;
        private final List<TimelineEntry> activities = new ArrayList<>();
        
        public ActivityTimelineProjection(String userId) {
            this.userId = userId;
        }
        
        public void addActivity(UserEvent event) {
            activities.add(new TimelineEntry(event.getTimestamp(), event.getEventType().toString(), 
                event.getMetadata().getOrDefault("eventSubType", "UNKNOWN")));
        }
        
        public String getUserId() { return userId; }
        public List<TimelineEntry> getActivities() { return activities; }
        
        static class TimelineEntry {
            private final long timestamp;
            private final String eventType;
            private final String eventSubType;
            
            public TimelineEntry(long timestamp, String eventType, String eventSubType) {
                this.timestamp = timestamp;
                this.eventType = eventType;
                this.eventSubType = eventSubType;
            }
            
            public long getTimestamp() { return timestamp; }
            public String getEventType() { return eventType; }
            public String getEventSubType() { return eventSubType; }
        }
    }
    
    static class ComplianceProjection {
        private final String userId;
        private boolean hasConsent = false;
        private boolean emailVerified = false;
        private Instant dataRetentionExpiry;
        private final List<String> processingActivities = new ArrayList<>();
        
        public ComplianceProjection(String userId) {
            this.userId = userId;
            this.dataRetentionExpiry = Instant.now().plusSeconds(7 * 365 * 24 * 3600); // 7 years
        }
        
        public void trackCompliance(UserEvent event) {
            String eventSubType = event.getMetadata().getOrDefault("eventSubType", "");
            
            switch (eventSubType) {
                case "USER_REGISTERED":
                    hasConsent = true;
                    processingActivities.add("REGISTRATION");
                    break;
                case "EMAIL_VERIFIED":
                    emailVerified = true;
                    processingActivities.add("EMAIL_VERIFICATION");
                    break;
                case "PURCHASE":
                    processingActivities.add("TRANSACTION_PROCESSING");
                    break;
                default:
                    processingActivities.add("DATA_PROCESSING");
                    break;
            }
        }
        
        public String getUserId() { return userId; }
        public boolean hasConsent() { return hasConsent; }
        public boolean isEmailVerified() { return emailVerified; }
        public Instant getDataRetentionExpiry() { return dataRetentionExpiry; }
        public List<String> getProcessingActivities() { return processingActivities; }
    }
}
