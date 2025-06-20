package com.example.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * Configuration factory for Kafka clients with advanced settings
 * Demonstrates cutting-edge Kafka configurations including:
 * - Exactly-once semantics
 * - Schema Registry integration
 * - Performance optimizations
 * - Security configurations
 */
public class KafkaConfigFactory {
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    
    /**
     * Creates producer configuration with exactly-once semantics
     */
    public static Properties createExactlyOnceProducerConfig() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        
        // Serialization - Use JSON for simplicity, Avro for production
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        // Exactly-once semantics configuration
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-showcase-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        
        // Performance optimizations
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768); // 32KB
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864); // 64MB
        
        // Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("auto.register.schemas", true);
        props.put("use.latest.version", true);
        
        return props;
    }
    
    /**
     * Creates consumer configuration for exactly-once processing
     */
    public static Properties createExactlyOnceConsumerConfig(String groupId) {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        
        // Exactly-once semantics
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Performance and reliability
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        // Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put("specific.avro.reader", true);
        
        return props;
    }
    
    /**
     * Creates Kafka Streams configuration with exactly-once processing
     */
    public static Properties createStreamsConfig(String applicationId) {
        Properties props = new Properties();
        
        // Basic configuration
        props.put("application.id", applicationId);
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        
        // Exactly-once processing
        props.put("processing.guarantee", "exactly_once_v2");
        props.put("replication.factor", 3);
        props.put("min.insync.replicas", 2);
        
        // Serialization
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde");
        
        // Schema Registry
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        
        // Performance tuning
        props.put("cache.max.bytes.buffering", 10485760); // 10MB
        props.put("commit.interval.ms", 30000); // 30 seconds
        props.put("num.stream.threads", Runtime.getRuntime().availableProcessors());
        
        return props;
    }
    
    /**
     * Creates admin client configuration
     */
    public static Properties createAdminConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("connections.max.idle.ms", 10000);
        props.put("request.timeout.ms", 5000);
        return props;
    }
    
    /**
     * Creates basic producer configuration with default settings
     */
    public static Properties createProducerConfig() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // Reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        
        return props;
    }
    
    /**
     * Creates basic consumer configuration with default settings
     */
    public static Properties createConsumerConfig() {
        Properties props = new Properties();
        
        // Basic configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // Consumer settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        
        return props;
    }
}
