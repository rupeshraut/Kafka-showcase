# Kafka Advanced Showcase

A comprehensive demonstration of cutting-edge Apache Kafka features and patterns, showcasing advanced capabilities for modern distributed systems.

## 🚀 Features

This project demonstrates the following advanced Kafka capabilities:

### 🔒 Exactly-Once Semantics
- **Transactional Producers**: Guaranteed exactly-once delivery with idempotent producers
- **Transactional Processing**: Consume-transform-produce patterns with full transaction support
- **Read Committed Isolation**: Consumers with read-committed isolation level

### 📊 Schema Management
- **Avro Serialization**: Type-safe serialization with schema evolution
- **Schema Registry Integration**: Centralized schema management and compatibility checking
- **Backward/Forward Compatibility**: Seamless schema evolution strategies

### 🌊 Stream Processing
- **Kafka Streams**: Real-time stream processing with stateful operations
- **Windowing Operations**: Tumbling, hopping, and session windows
- **Stream-Stream Joins**: Complex event correlation and enrichment
- **State Stores**: Persistent and in-memory state management

### ⚡ Performance Optimizations
- **Batch Processing**: Optimized batching for high throughput
- **Compression**: LZ4 compression for reduced network overhead
- **Memory Management**: Tuned buffer sizes and caching strategies
- **Parallel Processing**: Multi-threaded processing patterns

### 📈 Monitoring & Observability
- **Metrics Collection**: Comprehensive metrics with Micrometer
- **Prometheus Integration**: Time-series metrics collection
- **Grafana Dashboards**: Visual monitoring and alerting
- **JMX Monitoring**: Deep JVM and Kafka metrics

### 🛡️ Error Handling & Resilience
- **Retry Mechanisms**: Exponential backoff and circuit breakers
- **Dead Letter Queues**: Failed message handling patterns
- **Graceful Shutdown**: Clean resource cleanup and offset management
- **Health Checks**: Application and dependency health monitoring

### 🎯 Advanced Partitioning & Consumer Assignment
- **Custom Partitioners**: Geographic, temporal, user-tier, and hybrid partitioning strategies
- **Advanced Assignment**: Capacity-aware, priority-based, and cross-zone resilient consumer assignment
- **Load Balancing**: Sophisticated load distribution across heterogeneous consumers
- **Session Affinity**: Consistent hash partitioning for stateful processing

### 🔄 Consumer Rebalancing Patterns
- **E-commerce Scaling**: Black Friday traffic spike handling with geographic distribution
- **Financial Trading**: Priority-based rebalancing for critical vs non-critical trading data
- **IoT Fleet Management**: Zone-aware rebalancing with hot-standby consumers
- **Multi-tenant SaaS**: Tenant isolation with SLA-based resource allocation
- **Streaming Analytics**: ML model deployment with capacity-aware assignment

### 🌟 Cutting-Edge Use Cases
- **AI/ML Model Serving**: Real-time inference pipelines with GPU-aware processing
- **Autonomous Vehicles**: Fleet management with safety-critical event handling
- **Real-time Fraud Detection**: Financial transaction monitoring with risk scoring
- **Smart City Infrastructure**: IoT sensor networks with edge processing
- **Cryptocurrency Trading**: High-frequency trading with latency optimization

### 📚 Event Sourcing & CQRS
- **Event Store**: Kafka as immutable event log with complete audit trails
- **Command-Query Separation**: Separate read and write models for optimal performance
- **Event Replay**: Historical reconstruction and new projection building
- **Saga Patterns**: Distributed transaction coordination across aggregates
- **Compliance**: GDPR-compliant data handling with temporal queries

## 🏗️ Project Structure

```
kafka-showcase/
├── app/
│   ├── src/main/java/com/example/kafka/
│   │   ├── App.java                    # Main application with interactive menu
│   │   ├── config/
│   │   │   └── KafkaConfigFactory.java # Kafka configuration factory
│   │   ├── producers/
│   │   │   └── AdvancedProducerDemo.java # Advanced producer patterns
│   │   ├── consumers/
│   │   │   └── AdvancedConsumerDemo.java # Advanced consumer patterns
│   │   ├── streams/
│   │   │   ├── StreamsDemo.java        # Kafka Streams processing
│   │   │   └── UserStateTransformer.java # Custom transformer
│   │   ├── transactions/
│   │   │   └── TransactionDemo.java    # Transactional processing
│   │   ├── partitioning/
│   │   │   ├── AdvancedPartitioner.java      # Custom partitioner with multiple strategies
│   │   │   ├── AdvancedPartitionAssignor.java # Custom partition assignor
│   │   │   ├── PartitioningPatternsDemo.java # Partitioning patterns demonstration
│   │   │   └── ConsumerRebalancingPatternsDemo.java # Real-world rebalancing scenarios
│   │   ├── avro/
│   │   │   ├── User.java               # User data model
│   │   │   ├── UserEvent.java          # Event data model
│   │   │   └── EventType.java          # Event type enum
│   │   └── model/                      # Additional data models
│   ├── src/main/avro/
│   │   ├── user.avsc                   # User Avro schema
│   │   └── user_event.avsc             # User event Avro schema
│   ├── src/main/resources/
│   │   ├── application.conf            # Application configuration
│   │   └── logback.xml                 # Logging configuration
│   └── build.gradle                    # Build configuration
├── docker/
│   ├── docker-compose.yml              # Complete Kafka ecosystem
│   └── prometheus.yml                  # Prometheus configuration
└── README.md
```

## 🛠️ Prerequisites

- **Java 21** or higher
- **Docker** and **Docker Compose**
- **Gradle** (included wrapper)

## 🚀 Quick Start

### 1. Start the Kafka Ecosystem

```bash
# Start all services (Kafka, Schema Registry, Connect, etc.)
cd docker
docker-compose up -d

# Verify services are running
docker-compose ps
```

### 2. Build the Application

```bash
# Build the project
./gradlew build

# Verify compilation
./gradlew compileJava
```

### 3. Run the Application

#### Interactive Mode
```bash
./gradlew run
```

#### Specific Demo Mode
```bash
# Producer demo
./gradlew runProducer

# Consumer demo
./gradlew runConsumer

# Streams demo
./gradlew runStreams

# Transaction demo
./gradlew runTransactionDemo

# Consumer rebalancing patterns demo
java -jar app/build/libs/app.jar rebalancing

# Cutting-edge use cases demo
java -jar app/build/libs/app.jar cutting-edge

# Event sourcing and CQRS demo
java -jar app/build/libs/app.jar event-sourcing
```

## 📊 Monitoring and Visualization

After starting the Docker Compose stack, access these web interfaces:

- **Kafka UI**: http://localhost:8080 - Comprehensive Kafka cluster management
- **Control Center**: http://localhost:9021 - Confluent Control Center
- **Schema Registry**: http://localhost:8081 - Schema management API
- **Prometheus**: http://localhost:9090 - Metrics collection
- **Grafana**: http://localhost:3000 - Metrics visualization (admin/admin)

## 🎯 Demo Scenarios

### Producer Demo
Demonstrates:
- Exactly-once producer semantics
- Avro serialization with Schema Registry
- Asynchronous and synchronous production
- Batch processing for high throughput
- Error handling and retry mechanisms

### Consumer Demo
Demonstrates:
- Exactly-once consumer processing
- Manual offset management
- Consumer group coordination
- Error handling and recovery
- Metrics collection and reporting

### Streams Demo
Demonstrates:
- Real-time stream processing
- Windowing and aggregations
- Stream transformations and filtering
- State management
- Topology visualization

### Transaction Demo
Demonstrates:
- Consume-transform-produce pattern
- Transactional processing
- Offset management in transactions
- Error handling and rollback
- Exactly-once end-to-end processing

### Advanced Partitioning Demo
Demonstrates:
- **Geographic Partitioning**: Region-based data placement for compliance and latency
- **Temporal Partitioning**: Time-based partitioning for efficient time-series queries
- **User Tier Partitioning**: Service level differentiation with dedicated partitions
- **Consistent Hash Partitioning**: Session affinity for stateful processing
- **Load-Balanced Partitioning**: Even distribution across partitions
- **Hybrid Partitioning**: Multiple strategies based on message content

### Consumer Rebalancing Patterns Demo
Demonstrates real-world scenarios:
- **E-commerce Black Friday Scaling**: Dynamic consumer scaling during traffic spikes with geographic distribution
- **Financial Trading System**: Priority-based rebalancing for critical vs non-critical events with sticky assignment
- **IoT Fleet Management**: Zone-aware rebalancing with hot-standby consumers for fault tolerance
- **Multi-tenant SaaS Platform**: Tenant isolation with SLA-based resource allocation and tier changes
- **Streaming Analytics Pipeline**: ML model deployment with capacity-aware assignment for GPU/CPU optimization

### Cutting-Edge Use Cases Demo
Demonstrates next-generation patterns:
- **AI/ML Model Serving**: Real-time inference pipelines with dynamic model deployment
- **Autonomous Vehicle Fleet**: Safety-critical event processing with redundancy
- **Real-Time Fraud Detection**: Financial monitoring with risk-based prioritization
- **Smart City Infrastructure**: IoT sensor networks with edge computing
- **Cryptocurrency Trading**: High-frequency trading with ultra-low latency

### Event Sourcing & CQRS Demo
Demonstrates advanced architectural patterns:
- **Event Store Implementation**: Kafka as immutable event log with ordering guarantees
- **Command Processing**: Transactional command handling with event publication
- **Event Replay**: Historical state reconstruction and projection building
- **CQRS Projections**: Specialized read models for different query patterns
- **Saga Patterns**: Distributed transaction coordination with correlation tracking

### Consumer Assignment Patterns
Demonstrates:
- **Geographic Affinity**: Consumers process data from their region
- **Capacity-Aware Assignment**: Distribution based on consumer processing power
- **Priority-Based Assignment**: High-priority consumers get critical partitions
- **Cross-Zone Resilient**: Fault-tolerant assignment across availability zones
- **Sticky Assignment**: Minimize rebalancing while respecting priorities

## 🎯 Advanced Partitioning Usage

### Using Custom Partitioner

```java
// Configure producer with advanced partitioner
Properties props = KafkaConfigFactory.createProducerConfig();
props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());

// Geographic partitioning - key format: "REGION:COUNTRY:USER_ID"
String key = String.format("%s:%s:%d", "US", "USA", userId);
ProducerRecord<String, UserEvent> record = new ProducerRecord<>(topic, key, event);

// Temporal partitioning - key format: "HOUR_BUCKET:ENTITY_ID"
long hourBucket = timestamp / 3600000L;
String key = String.format("%d:%s", hourBucket, entityId);

// User tier partitioning - key format: "TIER:USER_ID"
String key = String.format("%s:%d", "VIP", userId);
```

### Using Custom Partition Assignor

```java
// Configure consumer with advanced partition assignor
Properties props = KafkaConfigFactory.createConsumerConfig();
props.put(ConsumerConfig.GROUP_ID_CONFIG, "advanced-consumer-group");
props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, 
    AdvancedPartitionAssignor.class.getName());

// Set consumer metadata for assignment strategy
props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-us-east-high-capacity");

// Consumer metadata format: "strategy=STRATEGY_NAME;param1=value1;param2=value2"
// Examples:
// "strategy=GEOGRAPHIC_AFFINITY;region=US"
// "strategy=CAPACITY_AWARE;capacity=4.0"
// "strategy=STICKY_PRIORITY;priority=10"
// "strategy=CROSS_ZONE_RESILIENT;zone=us-east-1a"
```

### Real-World Use Cases

#### Global E-Commerce Platform
```java
// Producer: Partition orders by region for compliance
String orderKey = String.format("%s:%s", customerRegion, orderId);

// Consumer: Geographic affinity assignment
String userData = "strategy=GEOGRAPHIC_AFFINITY;region=US";
```

#### High-Frequency Trading
```java
// Producer: Consistent hash for symbol-based processing
ProducerRecord<String, TradeEvent> record = new ProducerRecord<>(topic, symbol, trade);

// Consumer: Capacity-aware assignment for heterogeneous infrastructure
String userData = "strategy=CAPACITY_AWARE;capacity=8.0"; // 8x baseline capacity
```

#### IoT Data Pipeline
```java
// Producer: Temporal partitioning for time-series data
long hourBucket = timestamp / 3600000L;
String key = String.format("%d:%s", hourBucket, sensorId);

// Consumer: Cross-zone resilient assignment
String userData = "strategy=CROSS_ZONE_RESILIENT;zone=us-west-2a";
```

#### Multi-Tenant SaaS
```java
// Producer: User tier partitioning for SLA differentiation
String key = String.format("%s:%d", userTier, userId); // "ENTERPRISE:12345"

// Consumer: Priority-based assignment
String userData = "strategy=STICKY_PRIORITY;priority=10"; // High priority for enterprise
```

## 🔧 Configuration

### Kafka Configuration
Key configurations in `KafkaConfigFactory.java`:

```java
// Exactly-once producer
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-showcase-producer");
props.put(ProducerConfig.ACKS_CONFIG, "all");

// Read-committed consumer
props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

// Exactly-once streams
props.put("processing.guarantee", "exactly_once_v2");
```

### Application Configuration
Customize settings in `application.conf`:

```hocon
kafka {
  bootstrap.servers = "localhost:9092"
  schema.registry.url = "http://localhost:8081"
  
  producer {
    enable.idempotence = true
    acks = "all"
    compression.type = "lz4"
  }
}
```

## 📈 Performance Tuning

### Producer Optimizations
- **Batching**: Configured for optimal throughput with `batch.size` and `linger.ms`
- **Compression**: LZ4 compression for reduced network usage
- **Memory**: Tuned `buffer.memory` for high-volume scenarios
- **Idempotence**: Enabled for exactly-once delivery

### Consumer Optimizations
- **Fetch Settings**: Optimized `fetch.min.bytes` and `fetch.max.wait.ms`
- **Polling**: Configured `max.poll.records` for balanced processing
- **Isolation**: Read-committed for transactional consistency

### Streams Optimizations
- **Threading**: Optimized `num.stream.threads` based on available cores
- **Caching**: Configured `cache.max.bytes.buffering` for state stores
- **Commit Interval**: Tuned `commit.interval.ms` for performance vs. latency

## 🔍 Troubleshooting

### Common Issues

#### Schema Registry Connection
```bash
# Check Schema Registry health
curl http://localhost:8081/subjects

# Verify Kafka connectivity
./gradlew test
```

#### Consumer Lag
```bash
# Monitor consumer lag in Kafka UI
open http://localhost:8080

# Check consumer group status
docker exec kafka-showcase-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

#### Transaction Issues
```bash
# Check transaction coordinator logs
docker logs kafka-showcase-broker | grep -i transaction
```

### Logs and Debugging
- Application logs: `logs/kafka-showcase.log`
- Kafka broker logs: `docker logs kafka-showcase-broker`
- Schema Registry logs: `docker logs kafka-showcase-schema-registry`

## 🧪 Testing

### Unit Tests
```bash
./gradlew test
```

### Integration Tests
```bash
./gradlew integrationTest
```

### Performance Tests
```bash
./gradlew performanceTest
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## 📚 Learning Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Platform Documentation](https://docs.confluent.io/)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)

## 🏷️ Version History

- **v1.0.0** - Initial release with advanced Kafka patterns
  - Exactly-once semantics
  - Avro serialization
  - Kafka Streams
  - Transactional processing
  - Monitoring and observability

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Kafka community for the excellent documentation
- Confluent for the comprehensive platform
- Contributors and reviewers

---

**Built with ❤️ for the Kafka community**
