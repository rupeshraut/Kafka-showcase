package com.example.kafka.partitioning;

import com.example.kafka.config.KafkaConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Comprehensive demonstration of real-world consumer rebalancing scenarios and patterns.
 * 
 * This demo showcases critical rebalancing use cases across different industries:
 * 
 * 1. E-COMMERCE BLACK FRIDAY SCALING
 *    - Dynamic consumer scaling during traffic spikes
 *    - Geographic load distribution for global platforms
 *    - Graceful handling of consumer failures during peak times
 * 
 * 2. FINANCIAL TRADING SYSTEM REBALANCING
 *    - Market hours vs after-hours consumer capacity changes
 *    - Priority-based rebalancing for critical vs non-critical trading data
 *    - Risk management requiring immediate consumer role changes
 * 
 * 3. IOT/TELEMETRY FLEET MANAGEMENT
 *    - Device fleet expansion triggering consumer group growth
 *    - Zone-aware rebalancing for network partition tolerance
 *    - Hot-standby consumers for critical sensor data
 * 
 * 4. MULTI-TENANT SAAS PLATFORM
 *    - Tenant isolation requiring dedicated consumer assignments
 *    - SLA-based rebalancing for different service tiers
 *    - Resource scaling based on tenant activity patterns
 * 
 * 5. STREAMING ANALYTICS & ML PIPELINES
 *    - Model deployment requiring compute resource reallocation
 *    - Real-time vs batch processing capacity changes
 *    - Disaster recovery scenarios with cross-region failover
 */
public class ConsumerRebalancingPatternsDemo {
    
    private static final Logger logger = LoggerFactory.getLogger(ConsumerRebalancingPatternsDemo.class);
    
    // Topics for different use cases
    private static final String ECOMMERCE_ORDERS_TOPIC = "ecommerce-orders";
    private static final String TRADING_EVENTS_TOPIC = "trading-events";
    private static final String IOT_SENSORS_TOPIC = "iot-sensor-data";
    private static final String TENANT_EVENTS_TOPIC = "tenant-events";
    private static final String ANALYTICS_STREAM_TOPIC = "analytics-stream";
    
    /**
     * Main demonstration runner - showcases all rebalancing scenarios
     */
    public static void main(String[] args) {
        logger.info("=== KAFKA CONSUMER REBALANCING PATTERNS DEMO ===");
        
        try {
            // 1. E-commerce Black Friday scaling scenario
            demonstrateEcommerceScalingRebalancing();
            
            Thread.sleep(2000); // Brief pause between demos
            
            // 2. Financial trading system priority rebalancing
            demonstrateFinancialTradingRebalancing();
            
            Thread.sleep(2000);
            
            // 3. IoT fleet management with zone-aware rebalancing
            demonstrateIoTFleetRebalancing();
            
            Thread.sleep(2000);
            
            // 4. Multi-tenant SaaS platform rebalancing
            demonstrateMultiTenantRebalancing();
            
            Thread.sleep(2000);
            
            // 5. Streaming analytics ML pipeline rebalancing
            demonstrateAnalyticsPipelineRebalancing();
            
        } catch (Exception e) {
            logger.error("Error in rebalancing patterns demo", e);
        }
        
        logger.info("=== CONSUMER REBALANCING PATTERNS DEMO COMPLETED ===");
    }
    
    /**
     * Real-world use case: E-commerce platform during Black Friday
     * 
     * Scenario: Online retail platform experiences 10x traffic spike during Black Friday.
     * System needs to dynamically scale consumers across geographic regions while
     * maintaining order processing guarantees.
     * 
     * Rebalancing triggers:
     * - Traffic spike requiring more consumers
     * - Regional load imbalance needing redistribution
     * - Consumer failures during peak times
     */
    private static void demonstrateEcommerceScalingRebalancing() {
        logger.info("=== E-COMMERCE BLACK FRIDAY SCALING REBALANCING ===");
        
        // Create producer to simulate Black Friday order flow
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AdvancedPartitioner.class.getName());
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            // Simulate order burst
            logger.info("Simulating Black Friday order burst...");
            for (int i = 0; i < 20; i++) {
                String region = getRegion(i);
                String orderId = "order_" + (System.currentTimeMillis() + i);
                String orderData = String.format(
                    "{\"orderId\":\"%s\", \"region\":\"%s\", \"amount\":%.2f, \"priority\":\"%s\", \"timestamp\":%d}",
                    orderId, region, 100.0 + (i * 10), getPriority(i), System.currentTimeMillis()
                );
                
                String key = String.format("%s:%s", region, orderId);
                ProducerRecord<String, String> record = new ProducerRecord<>(ECOMMERCE_ORDERS_TOPIC, key, orderData);
                
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.debug("Order sent - Region: {}, Partition: {}, Offset: {}", 
                            region, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
        
        // Create consumer group with rebalancing
        List<Thread> consumerThreads = createEcommerceConsumerGroup();
        
        // Simulate consumer scaling
        simulateConsumerScaling(consumerThreads, "ecommerce-scaling-group");
        
        // Wait for consumers to process
        waitForConsumers(consumerThreads, 3000);
    }
    
    /**
     * Real-world use case: Financial trading system during market transitions
     * 
     * Scenario: Trading system needs different consumer capacity during market hours
     * vs after-hours. Critical trading events need priority processing with immediate
     * rebalancing when risk thresholds are breached.
     */
    private static void demonstrateFinancialTradingRebalancing() {
        logger.info("=== FINANCIAL TRADING PRIORITY REBALANCING ===");
        
        // Create trading event stream
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            logger.info("Simulating trading events with varying priorities...");
            String[] instruments = {"AAPL", "GOOGL", "TSLA", "BTC-USD", "EUR-USD"};
            String[] eventTypes = {"TRADE", "QUOTE", "RISK_ALERT", "SETTLEMENT"};
            
            for (int i = 0; i < 25; i++) {
                String instrument = instruments[i % instruments.length];
                String eventType = eventTypes[i % eventTypes.length];
                String priority = eventType.equals("RISK_ALERT") ? "CRITICAL" : "NORMAL";
                
                String eventData = String.format(
                    "{\"instrument\":\"%s\", \"type\":\"%s\", \"priority\":\"%s\", \"price\":%.2f, \"volume\":%d, \"timestamp\":%d}",
                    instrument, eventType, priority, 100.0 + (i * 2.5), (i + 1) * 100, System.currentTimeMillis()
                );
                
                String key = String.format("%s:%s", priority, instrument);
                ProducerRecord<String, String> record = new ProducerRecord<>(TRADING_EVENTS_TOPIC, key, eventData);
                
                producer.send(record);
            }
            producer.flush();
        }
        
        // Create priority-aware consumer group
        List<Thread> tradingConsumers = createTradingConsumerGroup();
        
        // Simulate priority escalation rebalancing
        simulatePriorityEscalation(tradingConsumers);
        
        waitForConsumers(tradingConsumers, 3000);
    }
    
    /**
     * Real-world use case: IoT device fleet with zone-aware processing
     * 
     * Scenario: Industrial IoT system with sensors across multiple availability zones.
     * System must maintain processing even if an entire zone fails, requiring
     * immediate rebalancing to surviving zones.
     */
    private static void demonstrateIoTFleetRebalancing() {
        logger.info("=== IOT FLEET ZONE-AWARE REBALANCING ===");
        
        // Simulate IoT sensor data
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            logger.info("Simulating IoT sensor data from multiple zones...");
            String[] zones = {"zone-a", "zone-b", "zone-c"};
            String[] sensorTypes = {"temperature", "humidity", "pressure", "vibration"};
            
            for (int i = 0; i < 30; i++) {
                String zone = zones[i % zones.length];
                String sensorType = sensorTypes[i % sensorTypes.length];
                String sensorId = String.format("sensor_%s_%03d", sensorType, i);
                
                String sensorData = String.format(
                    "{\"sensorId\":\"%s\", \"type\":\"%s\", \"zone\":\"%s\", \"value\":%.2f, \"critical\":%s, \"timestamp\":%d}",
                    sensorId, sensorType, zone, 20.0 + (i % 50), (i % 10 == 0), System.currentTimeMillis()
                );
                
                String key = String.format("%s:%s", zone, sensorId);
                ProducerRecord<String, String> record = new ProducerRecord<>(IOT_SENSORS_TOPIC, key, sensorData);
                
                producer.send(record);
            }
            producer.flush();
        }
        
        // Create zone-aware consumer group
        List<Thread> iotConsumers = createIoTConsumerGroup();
        
        // Simulate zone failure scenario
        simulateZoneFailure(iotConsumers);
        
        waitForConsumers(iotConsumers, 3000);
    }
    
    /**
     * Real-world use case: Multi-tenant SaaS platform with isolation requirements
     * 
     * Scenario: SaaS platform serving multiple tenants with different SLA tiers.
     * Gold tier tenants need dedicated consumers, while silver/bronze can share.
     * Rebalancing occurs when tenants upgrade/downgrade or during resource scaling.
     */
    private static void demonstrateMultiTenantRebalancing() {
        logger.info("=== MULTI-TENANT SAAS REBALANCING ===");
        
        // Create tenant event stream
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            logger.info("Simulating multi-tenant events with different SLAs...");
            String[] tenants = {"tenant_gold_1", "tenant_silver_1", "tenant_silver_2", "tenant_bronze_1", "tenant_bronze_2"};
            String[] eventTypes = {"user_action", "billing_event", "audit_log", "notification"};
            
            for (int i = 0; i < 35; i++) {
                String tenant = tenants[i % tenants.length];
                String eventType = eventTypes[i % eventTypes.length];
                String slaTier = extractSlaTier(tenant);
                
                String eventData = String.format(
                    "{\"tenant\":\"%s\", \"eventType\":\"%s\", \"slaTier\":\"%s\", \"userId\":\"user_%d\", \"timestamp\":%d}",
                    tenant, eventType, slaTier, i % 100, System.currentTimeMillis()
                );
                
                String key = String.format("%s:%s", slaTier, tenant);
                ProducerRecord<String, String> record = new ProducerRecord<>(TENANT_EVENTS_TOPIC, key, eventData);
                
                producer.send(record);
            }
            producer.flush();
        }
        
        // Create tenant-aware consumer group
        List<Thread> tenantConsumers = createTenantConsumerGroup();
        
        // Simulate tenant tier changes
        simulateTenantTierChanges(tenantConsumers);
        
        waitForConsumers(tenantConsumers, 3000);
    }
    
    /**
     * Real-world use case: Streaming analytics with ML model deployment
     * 
     * Scenario: Real-time analytics platform deploys new ML models requiring
     * different compute resources. Rebalancing redistributes partitions based
     * on model complexity and available GPU/CPU resources.
     */
    private static void demonstrateAnalyticsPipelineRebalancing() {
        logger.info("=== STREAMING ANALYTICS ML PIPELINE REBALANCING ===");
        
        // Create analytics stream
        Properties producerProps = KafkaConfigFactory.createProducerConfig();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
            
            logger.info("Simulating analytics stream with varying computational complexity...");
            String[] dataTypes = {"simple_metrics", "complex_analysis", "ml_inference", "batch_aggregation"};
            String[] models = {"linear_regression", "neural_network", "ensemble_model", "deep_learning"};
            
            for (int i = 0; i < 40; i++) {
                String dataType = dataTypes[i % dataTypes.length];
                String model = models[i % models.length];
                String complexity = getComputationalComplexity(dataType, model);
                
                String analyticsData = String.format(
                    "{\"dataType\":\"%s\", \"model\":\"%s\", \"complexity\":\"%s\", \"dataSize\":%d, \"requiredGpu\":%s, \"timestamp\":%d}",
                    dataType, model, complexity, (i + 1) * 1024, complexity.equals("HIGH"), System.currentTimeMillis()
                );
                
                String key = String.format("%s:%s", complexity, model);
                ProducerRecord<String, String> record = new ProducerRecord<>(ANALYTICS_STREAM_TOPIC, key, analyticsData);
                
                producer.send(record);
            }
            producer.flush();
        }
        
        // Create compute-aware consumer group
        List<Thread> analyticsConsumers = createAnalyticsConsumerGroup();
        
        // Simulate model deployment resource changes
        simulateModelDeploymentRebalancing(analyticsConsumers);
        
        waitForConsumers(analyticsConsumers, 3000);
    }
    
    // Helper methods for creating consumer groups with rebalancing
    
    private static List<Thread> createEcommerceConsumerGroup() {
        List<Thread> consumers = new ArrayList<>();
        String[] regions = {"US", "EU", "APAC"};
        
        for (String region : regions) {
            for (int i = 0; i < 2; i++) { // 2 consumers per region
                String consumerId = String.format("ecommerce-consumer-%s-%d", region, i);
                Thread consumerThread = createConsumerThread(
                    consumerId,
                    "ecommerce-scaling-group",
                    ECOMMERCE_ORDERS_TOPIC,
                    String.format("strategy=GEOGRAPHIC_AFFINITY;region=%s", region),
                    new EcommerceRebalanceListener(consumerId, region)
                );
                consumers.add(consumerThread);
                consumerThread.start();
            }
        }
        
        return consumers;
    }
    
    private static List<Thread> createTradingConsumerGroup() {
        List<Thread> consumers = new ArrayList<>();
        
        // Critical consumers for risk alerts
        for (int i = 0; i < 2; i++) {
            String consumerId = String.format("trading-critical-consumer-%d", i);
            Thread consumerThread = createConsumerThread(
                consumerId,
                "trading-priority-group",
                TRADING_EVENTS_TOPIC,
                "strategy=STICKY_PRIORITY;priority=10;role=critical",
                new TradingRebalanceListener(consumerId, "CRITICAL")
            );
            consumers.add(consumerThread);
            consumerThread.start();
        }
        
        // Normal consumers for regular trades
        for (int i = 0; i < 3; i++) {
            String consumerId = String.format("trading-normal-consumer-%d", i);
            Thread consumerThread = createConsumerThread(
                consumerId,
                "trading-priority-group",
                TRADING_EVENTS_TOPIC,
                "strategy=STICKY_PRIORITY;priority=5;role=normal",
                new TradingRebalanceListener(consumerId, "NORMAL")
            );
            consumers.add(consumerThread);
            consumerThread.start();
        }
        
        return consumers;
    }
    
    private static List<Thread> createIoTConsumerGroup() {
        List<Thread> consumers = new ArrayList<>();
        String[] zones = {"zone-a", "zone-b", "zone-c"};
        
        for (String zone : zones) {
            // Primary consumers
            String primaryId = String.format("iot-primary-%s", zone);
            Thread primaryThread = createConsumerThread(
                primaryId,
                "iot-resilient-group",
                IOT_SENSORS_TOPIC,
                String.format("strategy=CROSS_ZONE_RESILIENT;zone=%s;role=primary", zone),
                new IoTRebalanceListener(primaryId, zone, "PRIMARY")
            );
            consumers.add(primaryThread);
            primaryThread.start();
            
            // Standby consumers
            String standbyId = String.format("iot-standby-%s", zone);
            Thread standbyThread = createConsumerThread(
                standbyId,
                "iot-resilient-group",
                IOT_SENSORS_TOPIC,
                String.format("strategy=HOT_STANDBY;zone=%s;role=standby", zone),
                new IoTRebalanceListener(standbyId, zone, "STANDBY")
            );
            consumers.add(standbyThread);
            standbyThread.start();
        }
        
        return consumers;
    }
    
    private static List<Thread> createTenantConsumerGroup() {
        List<Thread> consumers = new ArrayList<>();
        
        // Gold tier dedicated consumers
        String goldConsumerId = "tenant-gold-dedicated";
        Thread goldThread = createConsumerThread(
            goldConsumerId,
            "tenant-aware-group",
            TENANT_EVENTS_TOPIC,
            "strategy=TENANT_AWARE;tenant=gold;sla.tier=GOLD",
            new TenantRebalanceListener(goldConsumerId, "GOLD")
        );
        consumers.add(goldThread);
        goldThread.start();
        
        // Silver tier consumers
        for (int i = 0; i < 2; i++) {
            String silverId = String.format("tenant-silver-consumer-%d", i);
            Thread silverThread = createConsumerThread(
                silverId,
                "tenant-aware-group",
                TENANT_EVENTS_TOPIC,
                "strategy=SLA_BASED;sla.tier=SILVER",
                new TenantRebalanceListener(silverId, "SILVER")
            );
            consumers.add(silverThread);
            silverThread.start();
        }
        
        // Bronze tier shared consumers
        for (int i = 0; i < 2; i++) {
            String bronzeId = String.format("tenant-bronze-consumer-%d", i);
            Thread bronzeThread = createConsumerThread(
                bronzeId,
                "tenant-aware-group",
                TENANT_EVENTS_TOPIC,
                "strategy=SLA_BASED;sla.tier=BRONZE",
                new TenantRebalanceListener(bronzeId, "BRONZE")
            );
            consumers.add(bronzeThread);
            bronzeThread.start();
        }
        
        return consumers;
    }
    
    private static List<Thread> createAnalyticsConsumerGroup() {
        List<Thread> consumers = new ArrayList<>();
        
        // High-capacity GPU consumers
        for (int i = 0; i < 2; i++) {
            String consumerId = String.format("analytics-gpu-consumer-%d", i);
            Thread consumerThread = createConsumerThread(
                consumerId,
                "analytics-capacity-group",
                ANALYTICS_STREAM_TOPIC,
                "strategy=CAPACITY_AWARE;capacity=4.0;hardware=gpu",
                new AnalyticsRebalanceListener(consumerId, "GPU", 4.0)
            );
            consumers.add(consumerThread);
            consumerThread.start();
        }
        
        // Medium-capacity CPU consumers
        for (int i = 0; i < 3; i++) {
            String consumerId = String.format("analytics-cpu-consumer-%d", i);
            Thread consumerThread = createConsumerThread(
                consumerId,
                "analytics-capacity-group",
                ANALYTICS_STREAM_TOPIC,
                "strategy=CAPACITY_AWARE;capacity=2.0;hardware=cpu",
                new AnalyticsRebalanceListener(consumerId, "CPU", 2.0)
            );
            consumers.add(consumerThread);
            consumerThread.start();
        }
        
        return consumers;
    }
    
    // Generic consumer thread creation
    private static Thread createConsumerThread(String consumerId, String groupId, String topic, 
                                             String userData, ConsumerRebalanceListener rebalanceListener) {
        return new Thread(() -> {
            Properties consumerProps = KafkaConfigFactory.createConsumerConfig();
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, AdvancedPartitionAssignor.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit for demo
            
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                consumer.subscribe(Collections.singletonList(topic), rebalanceListener);
                
                // Process messages for a limited time
                long endTime = System.currentTimeMillis() + 5000; // 5 seconds
                while (System.currentTimeMillis() < endTime) {
                    var records = consumer.poll(Duration.ofMillis(500));
                    
                    records.forEach(record -> {
                        logger.info("Consumer {} processed: Topic={}, Partition={}, Key={}", 
                            consumerId, record.topic(), record.partition(), record.key());
                    });
                    
                    if (!records.isEmpty()) {
                        consumer.commitSync(); // Commit processed records
                    }
                }
                
                logger.info("Consumer {} shutting down", consumerId);
                
            } catch (Exception e) {
                logger.error("Error in consumer {}", consumerId, e);
            }
        });
    }
    
    // Simulation methods for different rebalancing scenarios
    
    private static void simulateConsumerScaling(List<Thread> consumers, String groupId) {
        // Simulate adding a new consumer mid-processing (scaling up)
        new Thread(() -> {
            try {
                Thread.sleep(1500); // Wait for initial processing
                logger.info("=== SCALING EVENT: Adding new consumer to handle Black Friday load ===");
                
                String newConsumerId = "ecommerce-consumer-scale-out";
                Thread newConsumer = createConsumerThread(
                    newConsumerId,
                    groupId,
                    ECOMMERCE_ORDERS_TOPIC,
                    "strategy=GEOGRAPHIC_AFFINITY;region=US",
                    new EcommerceRebalanceListener(newConsumerId, "US")
                );
                consumers.add(newConsumer);
                newConsumer.start();
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    private static void simulatePriorityEscalation(List<Thread> consumers) {
        // Simulate risk alert triggering priority rebalancing
        new Thread(() -> {
            try {
                Thread.sleep(1000);
                logger.info("=== RISK ALERT: Escalating consumer priorities for critical events ===");
                // In a real system, this would update consumer metadata to trigger rebalancing
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    private static void simulateZoneFailure(List<Thread> consumers) {
        // Simulate zone failure requiring failover
        new Thread(() -> {
            try {
                Thread.sleep(1500);
                logger.info("=== ZONE FAILURE: Simulating zone-a failure, triggering failover ===");
                
                // Find and interrupt zone-a consumers to simulate failure
                consumers.stream()
                    .filter(t -> t.getName().contains("zone-a") && t.getName().contains("primary"))
                    .forEach(t -> {
                        logger.warn("Simulating failure of consumer: {}", t.getName());
                        t.interrupt();
                    });
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    private static void simulateTenantTierChanges(List<Thread> consumers) {
        // Simulate tenant upgrading tier requiring rebalancing
        new Thread(() -> {
            try {
                Thread.sleep(1200);
                logger.info("=== TENANT UPGRADE: Silver tenant upgrading to Gold, requiring dedicated resources ===");
                // In a real system, this would trigger consumer group rebalancing
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    private static void simulateModelDeploymentRebalancing(List<Thread> consumers) {
        // Simulate new ML model requiring different compute resources
        new Thread(() -> {
            try {
                Thread.sleep(1800);
                logger.info("=== MODEL DEPLOYMENT: New deep learning model requires GPU rebalancing ===");
                // In a real system, this would redistribute partitions to GPU consumers
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    // Utility methods
    
    private static String getRegion(int index) {
        String[] regions = {"US", "EU", "APAC"};
        return regions[index % regions.length];
    }
    
    private static String getPriority(int index) {
        return (index % 5 == 0) ? "HIGH" : "NORMAL";
    }
    
    private static String extractSlaTier(String tenant) {
        if (tenant.contains("gold")) return "GOLD";
        if (tenant.contains("silver")) return "SILVER";
        return "BRONZE";
    }
    
    private static String getComputationalComplexity(String dataType, String model) {
        if (model.contains("deep_learning") || model.contains("neural_network")) return "HIGH";
        if (model.contains("ensemble")) return "MEDIUM";
        return "LOW";
    }
    
    private static void waitForConsumers(List<Thread> consumers, int timeoutMs) {
        try {
            Thread.sleep(timeoutMs);
            
            // Interrupt all consumers to graceful shutdown
            consumers.forEach(Thread::interrupt);
            
            // Wait for graceful shutdown
            for (Thread consumer : consumers) {
                try {
                    consumer.join(1000); // Wait up to 1 second for each
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    
    // Custom ConsumerRebalanceListener implementations for different use cases
    
    /**
     * E-commerce rebalance listener - handles Black Friday scaling events
     */
    static class EcommerceRebalanceListener implements ConsumerRebalanceListener {
        private final String consumerId;
        private final String region;
        
        public EcommerceRebalanceListener(String consumerId, String region) {
            this.consumerId = consumerId;
            this.region = region;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("ECOMMERCE REBALANCING - Consumer {} ({}) losing partitions: {}", 
                consumerId, region, partitions.size());
            // In production: commit offsets, save state
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("ECOMMERCE REBALANCING - Consumer {} ({}) gaining partitions: {}", 
                consumerId, region, partitions.size());
            // In production: initialize state for new partitions
        }
    }
    
    /**
     * Trading system rebalance listener - handles priority changes
     */
    static class TradingRebalanceListener implements ConsumerRebalanceListener {
        private final String consumerId;
        private final String role;
        
        public TradingRebalanceListener(String consumerId, String role) {
            this.consumerId = consumerId;
            this.role = role;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("TRADING REBALANCING - {} consumer {} losing partitions: {}", 
                role, consumerId, partitions.size());
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("TRADING REBALANCING - {} consumer {} gaining partitions: {}", 
                role, consumerId, partitions.size());
        }
    }
    
    /**
     * IoT rebalance listener - handles zone failures and failover
     */
    static class IoTRebalanceListener implements ConsumerRebalanceListener {
        private final String consumerId;
        private final String zone;
        private final String role;
        
        public IoTRebalanceListener(String consumerId, String zone, String role) {
            this.consumerId = consumerId;
            this.zone = zone;
            this.role = role;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("IOT REBALANCING - {} consumer {} in {} losing partitions: {}", 
                role, consumerId, zone, partitions.size());
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("IOT REBALANCING - {} consumer {} in {} gaining partitions: {}", 
                role, consumerId, zone, partitions.size());
            if (role.equals("STANDBY") && !partitions.isEmpty()) {
                logger.warn("FAILOVER ACTIVATED - Standby consumer {} now processing partitions", consumerId);
            }
        }
    }
    
    /**
     * Tenant-aware rebalance listener - handles SLA tier changes
     */
    static class TenantRebalanceListener implements ConsumerRebalanceListener {
        private final String consumerId;
        private final String slaTier;
        
        public TenantRebalanceListener(String consumerId, String slaTier) {
            this.consumerId = consumerId;
            this.slaTier = slaTier;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("TENANT REBALANCING - {} tier consumer {} losing partitions: {}", 
                slaTier, consumerId, partitions.size());
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("TENANT REBALANCING - {} tier consumer {} gaining partitions: {}", 
                slaTier, consumerId, partitions.size());
        }
    }
    
    /**
     * Analytics rebalance listener - handles model deployment resource changes
     */
    static class AnalyticsRebalanceListener implements ConsumerRebalanceListener {
        private final String consumerId;
        private final String hardwareType;
        private final double capacity;
        
        public AnalyticsRebalanceListener(String consumerId, String hardwareType, double capacity) {
            this.consumerId = consumerId;
            this.hardwareType = hardwareType;
            this.capacity = capacity;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("ANALYTICS REBALANCING - {} consumer {} (capacity: {}) losing partitions: {}", 
                hardwareType, consumerId, capacity, partitions.size());
        }
        
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("ANALYTICS REBALANCING - {} consumer {} (capacity: {}) gaining partitions: {}", 
                hardwareType, consumerId, capacity, partitions.size());
        }
    }
}
