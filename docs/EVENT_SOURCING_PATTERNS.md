# Event Sourcing and CQRS Patterns with Kafka

This document describes the advanced Event Sourcing and Command Query Responsibility Segregation (CQRS) patterns implemented in the Kafka Showcase project.

## Table of Contents

1. [Overview](#overview)
2. [Event Sourcing Fundamentals](#event-sourcing-fundamentals)
3. [CQRS Implementation](#cqrs-implementation)
4. [Kafka as Event Store](#kafka-as-event-store)
5. [Advanced Patterns](#advanced-patterns)
6. [Real-World Use Cases](#real-world-use-cases)
7. [Implementation Details](#implementation-details)
8. [Best Practices](#best-practices)

## Overview

Event Sourcing is a powerful architectural pattern where the state of an application is determined by a sequence of events stored in an immutable event log. Combined with CQRS, it provides exceptional scalability, auditability, and flexibility for complex business domains.

## Event Sourcing Fundamentals

### Core Concepts

#### Event Store
- **Immutable Event Log**: All state changes are captured as events
- **Append-Only**: Events are never modified or deleted
- **Ordering Guarantees**: Events are ordered within aggregate boundaries
- **Versioning**: Each event has a version number for consistency

#### Aggregates
- **Business Entities**: Domain objects that encapsulate business logic
- **Event Application**: State is reconstructed by replaying events
- **Command Handling**: Process commands and emit events
- **Consistency Boundaries**: Transactions span single aggregates

#### Projections
- **Read Models**: Materialized views optimized for queries
- **Event Handlers**: Process events to update projections
- **Eventually Consistent**: Updates are asynchronous
- **Multiple Views**: Different projections for different use cases

### Benefits

1. **Complete Audit Trail**: Every state change is recorded
2. **Temporal Queries**: Query state at any point in time
3. **Event Replay**: Rebuild state or create new projections
4. **Scalability**: Read and write operations can be optimized independently
5. **Flexibility**: Easy to add new features without data migration

## CQRS Implementation

### Command Side

```java
// Command processing with event storage
public class UserCommandHandler {
    public void handle(CreateUserCommand command) {
        UserAggregate user = new UserAggregate(command.getUserId());
        UserCreatedEvent event = user.createUser(command);
        eventStore.appendEvent(user.getId(), event);
    }
}
```

### Query Side

```java
// Specialized projections for different query patterns
public class CustomerSummaryProjection {
    public void handle(UserCreatedEvent event) {
        // Update customer summary view
    }
    
    public void handle(PurchaseCompletedEvent event) {
        // Update lifetime value and purchase history
    }
}
```

### Event Processing

```java
// Event handler for projection updates
@EventHandler
public void on(UserEvent event) {
    switch (event.getEventType()) {
        case USER_CREATED:
            updateUserProjection(event);
            break;
        case PURCHASE:
            updateCustomerSummary(event);
            updateActivityTimeline(event);
            break;
    }
}
```

## Kafka as Event Store

### Topic Design

#### Event Store Topic
- **Partition by Aggregate ID**: Ensures ordering within aggregate
- **Retention Policy**: Long-term or infinite retention
- **Replication Factor**: High replication for durability
- **Cleanup Policy**: Compact to preserve latest events

#### Snapshot Topic
- **Performance Optimization**: Avoid replaying all events
- **Configurable Frequency**: Snapshot every N events
- **Compression**: Efficient storage of aggregate state
- **Versioning**: Handle schema evolution

#### Projection Topics
- **Specialized Views**: One topic per projection type
- **Optimized Structure**: Schema optimized for query patterns
- **Consumer Groups**: Multiple consumers for scaling
- **Cleanup Policy**: Delete old records if not needed for history

### Producer Configuration

```java
// Exactly-once semantics for event publishing
Properties props = new Properties();
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "event-store-producer");
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
```

### Consumer Configuration

```java
// Read-committed isolation for consistent reads
Properties props = new Properties();
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
```

## Advanced Patterns

### Saga Pattern

Distributed transaction management across multiple aggregates:

```java
public class OrderSaga {
    @SagaStart
    public void handle(OrderPlacedEvent event) {
        // Step 1: Reserve inventory
        commandGateway.send(new ReserveInventoryCommand(event.getOrderId()));
    }
    
    @SagaHandler
    public void handle(InventoryReservedEvent event) {
        // Step 2: Process payment
        commandGateway.send(new ProcessPaymentCommand(event.getOrderId()));
    }
    
    @SagaHandler
    public void handle(PaymentProcessedEvent event) {
        // Step 3: Ship order
        commandGateway.send(new ShipOrderCommand(event.getOrderId()));
    }
}
```

### Event Correlation

Track related events across aggregate boundaries:

```java
public class CorrelatedEvent {
    private String correlationId;  // Links related events
    private String causationId;    // Points to causing event
    private String sagaId;         // Saga instance identifier
    private long sequenceNumber;   // Ordering within correlation
}
```

### Idempotency

Ensure commands can be safely retried:

```java
public class IdempotentCommandHandler {
    public void handle(CreateUserCommand command) {
        if (eventStore.hasEvent(command.getCommandId())) {
            // Command already processed, return existing result
            return eventStore.getEventByCommandId(command.getCommandId());
        }
        
        // Process command normally
        UserCreatedEvent event = processCommand(command);
        event.setCommandId(command.getCommandId());
        eventStore.appendEvent(event);
    }
}
```

### Snapshot Management

Optimize performance with periodic state snapshots:

```java
public class SnapshotManager {
    private static final int SNAPSHOT_FREQUENCY = 100;
    
    public UserAggregate loadAggregate(String aggregateId) {
        Snapshot snapshot = snapshotStore.getLatestSnapshot(aggregateId);
        List<Event> events = eventStore.getEventsAfter(aggregateId, snapshot.getVersion());
        
        UserAggregate aggregate = snapshot.reconstructAggregate();
        events.forEach(aggregate::applyEvent);
        
        return aggregate;
    }
    
    public void createSnapshotIfNeeded(UserAggregate aggregate) {
        if (aggregate.getVersion() % SNAPSHOT_FREQUENCY == 0) {
            Snapshot snapshot = aggregate.createSnapshot();
            snapshotStore.saveSnapshot(snapshot);
        }
    }
}
```

## Real-World Use Cases

### Financial Trading System

```java
// Trade execution with complete audit trail
public class TradeAggregate {
    public TradeSubmittedEvent submitTrade(SubmitTradeCommand command) {
        validateTrade(command);
        return new TradeSubmittedEvent(command.getTradeId(), command.getSymbol(), 
            command.getQuantity(), command.getPrice(), Instant.now());
    }
    
    public TradeExecutedEvent executeTrade(ExecuteTradeCommand command) {
        validateExecution(command);
        return new TradeExecutedEvent(command.getTradeId(), command.getExecutionPrice(), 
            command.getExecutionTime(), command.getMarketId());
    }
}
```

### E-commerce Order Processing

```java
// Order lifecycle with state transitions
public class OrderAggregate {
    private OrderStatus status = OrderStatus.PENDING;
    private List<OrderItem> items = new ArrayList<>();
    private PaymentInfo payment;
    
    public OrderCreatedEvent createOrder(CreateOrderCommand command) {
        this.items = command.getItems();
        this.status = OrderStatus.CREATED;
        return new OrderCreatedEvent(command.getOrderId(), items, command.getCustomerId());
    }
    
    public PaymentProcessedEvent processPayment(ProcessPaymentCommand command) {
        if (status != OrderStatus.CREATED) {
            throw new InvalidOrderStateException("Cannot process payment for order in state: " + status);
        }
        
        this.payment = command.getPaymentInfo();
        this.status = OrderStatus.PAID;
        return new PaymentProcessedEvent(getId(), payment.getAmount(), payment.getMethod());
    }
}
```

### User Management with GDPR Compliance

```java
// User data with privacy compliance tracking
public class UserAggregate {
    private ConsentStatus consentStatus = ConsentStatus.PENDING;
    private Set<DataProcessingActivity> activities = new HashSet<>();
    private Instant dataRetentionExpiry;
    
    public ConsentGrantedEvent grantConsent(GrantConsentCommand command) {
        this.consentStatus = ConsentStatus.GRANTED;
        this.dataRetentionExpiry = Instant.now().plus(command.getRetentionPeriod());
        return new ConsentGrantedEvent(getId(), command.getConsentTypes(), dataRetentionExpiry);
    }
    
    public DataErasedEvent eraseData(EraseDataCommand command) {
        validateErasureRequest(command);
        return new DataErasedEvent(getId(), command.getDataCategories(), Instant.now());
    }
}
```

## Implementation Details

### Event Schema Design

```java
public class BaseEvent {
    private String eventId;        // Unique event identifier
    private String aggregateId;    // Aggregate instance ID
    private String eventType;      // Event type for deserialization
    private long version;          // Event version within aggregate
    private Instant timestamp;     // Event occurrence time
    private String correlationId;  // Correlation with other events
    private String causationId;    // Causation chain
    private Map<String, Object> metadata; // Additional context
}
```

### Event Store Interface

```java
public interface EventStore {
    void appendEvent(String aggregateId, Event event);
    void appendEvents(String aggregateId, List<Event> events);
    List<Event> getEvents(String aggregateId);
    List<Event> getEventsAfter(String aggregateId, long version);
    List<Event> getEventsBetween(String aggregateId, long fromVersion, long toVersion);
    EventStream getEventStream(String aggregateId);
}
```

### Projection Manager

```java
public class ProjectionManager {
    private final Map<Class<?>, List<EventHandler>> handlers = new HashMap<>();
    
    public void registerHandler(Class<?> eventType, EventHandler handler) {
        handlers.computeIfAbsent(eventType, k -> new ArrayList<>()).add(handler);
    }
    
    public void processEvent(Event event) {
        List<EventHandler> eventHandlers = handlers.get(event.getClass());
        if (eventHandlers != null) {
            eventHandlers.forEach(handler -> handler.handle(event));
        }
    }
}
```

## Best Practices

### Event Design

1. **Immutable Events**: Events should never change once created
2. **Business Language**: Use domain-specific event names
3. **Rich Events**: Include sufficient data to avoid lookups
4. **Versioning Strategy**: Plan for schema evolution from the start
5. **Correlation IDs**: Track related events across boundaries

### Performance Optimization

1. **Snapshotting**: Implement for aggregates with many events
2. **Parallel Processing**: Use multiple consumers for projections
3. **Caching**: Cache frequently accessed projections
4. **Indexing**: Create appropriate indexes on projection tables
5. **Partitioning**: Partition by aggregate ID for scalability

### Error Handling

1. **Poison Messages**: Handle corrupted or invalid events
2. **Dead Letter Queues**: Route failed events for analysis
3. **Retry Logic**: Implement exponential backoff for transient failures
4. **Circuit Breakers**: Protect against cascade failures
5. **Monitoring**: Track event processing metrics and lag

### Security and Compliance

1. **Encryption**: Encrypt sensitive data in events
2. **Access Control**: Implement proper authorization
3. **Audit Logging**: Log all access to event streams
4. **Data Retention**: Implement appropriate retention policies
5. **GDPR Compliance**: Handle right to erasure carefully

### Testing Strategies

1. **Given-When-Then**: Test aggregates with event sequences
2. **Projection Testing**: Verify projections against event streams
3. **Integration Testing**: Test complete event flows
4. **Performance Testing**: Validate under load
5. **Chaos Testing**: Test failure scenarios

## Configuration Examples

### Kafka Topic Configuration

```bash
# Event store topic with infinite retention
kafka-topics --create --topic event-store \
  --partitions 12 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config retention.ms=-1 \
  --config segment.ms=86400000

# Snapshot topic with compression
kafka-topics --create --topic snapshots \
  --partitions 12 \
  --replication-factor 3 \
  --config compression.type=lz4 \
  --config cleanup.policy=compact

# Projection topics with shorter retention
kafka-topics --create --topic user-projection \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=604800000 # 7 days
```

### Producer Configuration

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "event-store-producer");
props.put(ProducerConfig.ACKS_CONFIG, "all");
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
```

### Consumer Configuration

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "projection-builder");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
```

## Conclusion

Event Sourcing with Kafka provides a robust foundation for building scalable, auditable, and flexible distributed systems. The patterns demonstrated in this showcase cover the essential concepts and advanced techniques needed for real-world implementations.

The combination of Kafka's durability guarantees, CQRS's separation of concerns, and event sourcing's audit capabilities creates a powerful architecture suitable for mission-critical applications in finance, e-commerce, healthcare, and other domains requiring strong consistency and compliance.
