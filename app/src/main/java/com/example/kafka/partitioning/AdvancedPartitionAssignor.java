package com.example.kafka.partitioning;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Advanced partition assignor implementing sophisticated assignment strategies for real-world use cases:
 * 
 * REBALANCING USE CASES:
 * 
 * 1. E-COMMERCE PLATFORM SCALING
 *    - Black Friday traffic spikes requiring dynamic consumer scaling
 *    - Geographic expansion with region-specific consumers
 *    - Product catalog updates triggering rebalancing
 * 
 * 2. FINANCIAL TRADING SYSTEMS  
 *    - Market hours vs after-hours processing capacity changes
 *    - Risk management requiring immediate consumer priority shifts
 *    - Regulatory compliance mandating specific partition ownership
 * 
 * 3. IOT/TELEMETRY PROCESSING
 *    - Device fleet expansion triggering consumer group growth
 *    - Network partition tolerance requiring zone-aware assignment
 *    - Data retention policies affecting partition lifecycle
 * 
 * 4. STREAMING ANALYTICS PLATFORMS
 *    - Machine learning model deployment requiring compute reallocation
 *    - Real-time dashboard requirements changing processing priorities
 *    - Data pipeline failures requiring failover consumers
 * 
 * 5. MULTI-TENANT SAAS PLATFORMS
 *    - Tenant resource limits affecting partition assignment
 *    - Service level agreements requiring priority consumers
 *    - Resource scaling based on tenant activity patterns
 * 
 * ASSIGNMENT STRATEGIES:
 * 
 * 1. Geographic Affinity Assignment - consumers process partitions from their region
 * 2. Load-Based Assignment - balance processing load across consumers  
 * 3. Sticky Assignment with Priorities - minimize reassignments while respecting priorities
 * 4. Capacity-Aware Assignment - assign based on consumer processing capacity
 * 5. Cross-Zone Resilient Assignment - ensure fault tolerance across availability zones
 * 6. Hot-Standby Assignment - maintain backup consumers for critical partitions
 * 7. Tenant-Aware Assignment - isolate processing by tenant boundaries
 * 8. SLA-Based Assignment - prioritize assignment based on service level requirements
 */
public class AdvancedPartitionAssignor extends AbstractPartitionAssignor {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedPartitionAssignor.class);
    
    // Configuration keys for assignment strategies
    private static final String ASSIGNMENT_STRATEGY_CONFIG = "assignment.strategy";
    private static final String CONSUMER_REGION_CONFIG = "consumer.region";
    private static final String CONSUMER_ZONE_CONFIG = "consumer.zone";
    private static final String CONSUMER_CAPACITY_CONFIG = "consumer.capacity";
    private static final String CONSUMER_PRIORITY_CONFIG = "consumer.priority";
    private static final String CONSUMER_TENANT_CONFIG = "consumer.tenant";
    private static final String CONSUMER_SLA_TIER_CONFIG = "consumer.sla.tier";
    private static final String CONSUMER_ROLE_CONFIG = "consumer.role"; // primary, standby
    
    public enum AssignmentStrategy {
        GEOGRAPHIC_AFFINITY,     // Region-based assignment for latency optimization
        LOAD_BALANCED,          // Equal load distribution
        STICKY_PRIORITY,        // Minimize reassignment with priorities  
        CAPACITY_AWARE,         // Assignment based on consumer capacity
        CROSS_ZONE_RESILIENT,   // Fault-tolerant across zones
        HOT_STANDBY,           // Maintain backup consumers for critical partitions
        TENANT_AWARE,          // Multi-tenant isolation
        SLA_BASED             // Service level agreement prioritization
    }
    
    /**
     * Real-world rebalancing scenarios tracking
     */
    public enum RebalancingTrigger {
        CONSUMER_JOIN,           // New consumer joining (scaling up)
        CONSUMER_LEAVE,          // Consumer leaving/failing (scaling down)
        PARTITION_ADDED,         // Topic partition increase
        PRIORITY_CHANGE,         // Consumer priority modification
        CAPACITY_CHANGE,         // Consumer capacity update
        ZONE_FAILURE,           // Availability zone failure
        TENANT_MIGRATION,       // Tenant moving between consumers
        SLA_VIOLATION,          // Service level breach requiring rebalancing
        MAINTENANCE_WINDOW,     // Planned maintenance requiring gradual migration
        DISASTER_RECOVERY      // Disaster recovery scenario
    }
    
    @Override
    public String name() {
        return "advanced";
    }
    
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                   Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("=== CONSUMER REBALANCING EVENT ===");
        logger.info("Starting advanced partition assignment for {} consumers and {} topics", 
            subscriptions.size(), partitionsPerTopic.size());
        
        // Detect and log rebalancing trigger
        RebalancingTrigger trigger = detectRebalancingTrigger(subscriptions);
        logger.info("Rebalancing triggered by: {}", trigger);
        
        // Determine assignment strategy from consumer configurations
        AssignmentStrategy strategy = determineAssignmentStrategy(subscriptions);
        logger.info("Using assignment strategy: {}", strategy);
        
        Map<String, List<TopicPartition>> assignment;
        
        switch (strategy) {
            case GEOGRAPHIC_AFFINITY:
                assignment = geographicAffinityAssignment(partitionsPerTopic, subscriptions);
                break;
            case LOAD_BALANCED:
                assignment = loadBalancedAssignment(partitionsPerTopic, subscriptions);
                break;
            case STICKY_PRIORITY:
                assignment = stickyPriorityAssignment(partitionsPerTopic, subscriptions);
                break;
            case CAPACITY_AWARE:
                assignment = capacityAwareAssignment(partitionsPerTopic, subscriptions);
                break;
            case CROSS_ZONE_RESILIENT:
                assignment = crossZoneResilientAssignment(partitionsPerTopic, subscriptions);
                break;
            case HOT_STANDBY:
                assignment = hotStandbyAssignment(partitionsPerTopic, subscriptions);
                break;
            case TENANT_AWARE:
                assignment = tenantAwareAssignment(partitionsPerTopic, subscriptions);
                break;
            case SLA_BASED:
                assignment = slaBasedAssignment(partitionsPerTopic, subscriptions);
                break;
            default:
                assignment = roundRobinAssignment(partitionsPerTopic, subscriptions);
        }
        
        // Log rebalancing impact
        logRebalancingImpact(trigger, strategy, assignment);
        
        return assignment;
    }
    
    /**
     * Detects what triggered the current rebalancing event
     * 
     * Real-world scenarios:
     * - E-commerce: Black Friday scaling events
     * - Trading: Market open/close capacity changes  
     * - IoT: Device fleet expansion
     * - Analytics: ML model deployment resource changes
     */
    private RebalancingTrigger detectRebalancingTrigger(
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        // In a real implementation, this would analyze:
        // - Previous consumer group membership
        // - Consumer metadata changes
        // - System metrics and alerts
        // - External triggers from management systems
        
        // For demo purposes, we'll infer from consumer patterns
        Set<String> consumerRoles = subscriptions.values().stream()
            .map(s -> extractUserDataValue(s, "role", "primary"))
            .collect(Collectors.toSet());
        
        if (consumerRoles.contains("standby")) {
            return RebalancingTrigger.DISASTER_RECOVERY;
        }
        
        Set<String> priorities = subscriptions.values().stream()
            .map(s -> extractUserDataValue(s, "priority", "1"))
            .collect(Collectors.toSet());
        
        if (priorities.size() > 2) {
            return RebalancingTrigger.PRIORITY_CHANGE;
        }
        
        if (subscriptions.size() > 5) {
            return RebalancingTrigger.CONSUMER_JOIN;
        }
        
        return RebalancingTrigger.CONSUMER_JOIN; // Default
    }

    /**
     * NEW: Hot-Standby Assignment
     * 
     * Real-world use case: Critical financial trading systems
     * 
     * Scenario: Each critical partition must have both a primary and standby consumer
     * to ensure zero-downtime processing during failures or maintenance.
     * 
     * Strategy: Assign each partition to both a primary and standby consumer
     * in different zones for maximum resilience.
     */
    private Map<String, List<TopicPartition>> hotStandbyAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing hot-standby assignment for high-availability requirements");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, String> consumerRoles = extractConsumerRoles(subscriptions);
        Map<String, String> consumerZones = extractConsumerZones(subscriptions);
        
        // Separate primary and standby consumers
        List<String> primaryConsumers = consumerRoles.entrySet().stream()
            .filter(entry -> "primary".equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        List<String> standbyConsumers = consumerRoles.entrySet().stream()
            .filter(entry -> "standby".equals(entry.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            for (int partition = 0; partition < partitionCount; partition++) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                
                // Assign to primary consumer
                if (!primaryConsumers.isEmpty()) {
                    String primaryConsumer = primaryConsumers.get(partition % primaryConsumers.size());
                    assignment.get(primaryConsumer).add(topicPartition);
                    
                    // Find standby consumer in different zone
                    String primaryZone = consumerZones.get(primaryConsumer);
                    String standbyConsumer = findStandbyInDifferentZone(standbyConsumers, primaryZone, consumerZones);
                    
                    if (standbyConsumer != null) {
                        assignment.get(standbyConsumer).add(topicPartition);
                        logger.debug("Partition {}-{} assigned to primary: {} (zone: {}) and standby: {} (zone: {})",
                            topic, partition, primaryConsumer, primaryZone, 
                            standbyConsumer, consumerZones.get(standbyConsumer));
                    }
                }
            }
        }
        
        logAssignmentSummary("Hot-Standby", assignment);
        return assignment;
    }
    
    /**
     * NEW: Tenant-Aware Assignment
     * 
     * Real-world use case: Multi-tenant SaaS platform
     * 
     * Scenario: Different tenants have different data isolation requirements,
     * SLAs, and resource allocations. Some tenants require dedicated consumers
     * for compliance or performance reasons.
     * 
     * Strategy: Isolate tenant data processing and allocate resources
     * based on tenant tiers and requirements.
     */
    private Map<String, List<TopicPartition>> tenantAwareAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing tenant-aware assignment for multi-tenant isolation");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, String> consumerTenants = extractConsumerTenants(subscriptions);
        
        // Group consumers by tenant
        Map<String, List<String>> tenantConsumers = new HashMap<>();
        for (Map.Entry<String, String> entry : consumerTenants.entrySet()) {
            String consumer = entry.getKey();
            String tenant = entry.getValue();
            tenantConsumers.computeIfAbsent(tenant, k -> new ArrayList<>()).add(consumer);
        }
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            // Determine tenant from topic name (e.g., tenant1-events, tenant2-logs)
            String topicTenant = extractTenantFromTopic(topic);
            
            for (int partition = 0; partition < partitionCount; partition++) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                
                // Assign to tenant-specific consumers first
                List<String> dedicatedConsumers = tenantConsumers.get(topicTenant);
                if (dedicatedConsumers != null && !dedicatedConsumers.isEmpty()) {
                    String consumer = dedicatedConsumers.get(partition % dedicatedConsumers.size());
                    assignment.get(consumer).add(topicPartition);
                    
                    logger.debug("Partition {}-{} assigned to dedicated consumer {} for tenant {}",
                        topic, partition, consumer, topicTenant);
                } else {
                    // Fallback to shared consumers for tenants without dedicated resources
                    List<String> sharedConsumers = tenantConsumers.get("shared");
                    if (sharedConsumers != null && !sharedConsumers.isEmpty()) {
                        String consumer = sharedConsumers.get(partition % sharedConsumers.size());
                        assignment.get(consumer).add(topicPartition);
                        
                        logger.debug("Partition {}-{} assigned to shared consumer {} for tenant {}",
                            topic, partition, consumer, topicTenant);
                    }
                }
            }
        }
        
        logAssignmentSummary("Tenant-Aware", assignment);
        logTenantDistribution(consumerTenants, assignment);
        return assignment;
    }
    
    /**
     * NEW: SLA-Based Assignment
     * 
     * Real-world use case: Service provider with tiered SLAs
     * 
     * Scenario: Different customers have different SLA requirements
     * (Gold: <1s processing, Silver: <5s, Bronze: <30s).
     * Higher SLA tiers need more powerful consumers and priority assignment.
     * 
     * Strategy: Assign partitions containing high-SLA data to high-performance
     * consumers with priority processing capabilities.
     */
    private Map<String, List<TopicPartition>> slaBasedAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing SLA-based assignment for service level guarantees");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, String> consumerSlaTiers = extractConsumerSlaTiers(subscriptions);
        
        // Group consumers by SLA tier (Gold > Silver > Bronze)
        Map<String, List<String>> tierConsumers = new HashMap<>();
        for (Map.Entry<String, String> entry : consumerSlaTiers.entrySet()) {
            String consumer = entry.getKey();
            String tier = entry.getValue();
            tierConsumers.computeIfAbsent(tier, k -> new ArrayList<>()).add(consumer);
        }
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        // Process partitions in SLA priority order
        String[] slaOrder = {"GOLD", "SILVER", "BRONZE"};
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            for (int partition = 0; partition < partitionCount; partition++) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                
                // Infer SLA requirement from partition (in real world, this would come from metadata)
                String requiredSla = inferPartitionSla(topic, partition, partitionCount);
                
                // Find appropriate consumer tier for this SLA
                String assignedConsumer = null;
                for (String sla : slaOrder) {
                    if (sla.equals(requiredSla) || isHigherSla(sla, requiredSla)) {
                        List<String> availableConsumers = tierConsumers.get(sla);
                        if (availableConsumers != null && !availableConsumers.isEmpty()) {
                            // Select least loaded consumer in this tier
                            assignedConsumer = availableConsumers.stream()
                                .min(Comparator.comparing(c -> assignment.get(c).size()))
                                .orElse(availableConsumers.get(0));
                            break;
                        }
                    }
                }
                
                if (assignedConsumer != null) {
                    assignment.get(assignedConsumer).add(topicPartition);
                    logger.debug("Partition {}-{} (SLA: {}) assigned to consumer {} (tier: {})",
                        topic, partition, requiredSla, assignedConsumer, consumerSlaTiers.get(assignedConsumer));
                }
            }
        }
        
        logAssignmentSummary("SLA-Based", assignment);
        logSlaDistribution(consumerSlaTiers, assignment);
        return assignment;
    }

    /**
     * Geographic Affinity Assignment
     * 
     * Real-world use case: Global e-commerce platform during Black Friday
     * 
     * Scenario: Users from different regions (US, EU, APAC) generate events.
     * Requirements: Events should be processed by consumers in the same region
     * for latency reduction and data residency compliance.
     * 
     * Rebalancing triggers:
     * - New regions coming online (geographic expansion)
     * - Regional traffic spikes requiring more consumers
     * - Data center maintenance requiring failover
     */
    private Map<String, List<TopicPartition>> geographicAffinityAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing geographic affinity assignment for global latency optimization");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, String> consumerRegions = extractConsumerRegions(subscriptions);
        
        // Initialize empty assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            // Group partitions by inferred region (based on partition number ranges)
            Map<String, List<TopicPartition>> regionPartitions = groupPartitionsByRegion(topic, partitionCount);
            
            // Assign region partitions to consumers in the same region
            for (Map.Entry<String, List<TopicPartition>> regionEntry : regionPartitions.entrySet()) {
                String region = regionEntry.getKey();
                List<TopicPartition> partitions = regionEntry.getValue();
                
                List<String> regionConsumers = consumerRegions.entrySet().stream()
                    .filter(entry -> region.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
                
                if (regionConsumers.isEmpty()) {
                    // Fallback: assign to any available consumer
                    regionConsumers = new ArrayList<>(subscriptions.keySet());
                    logger.warn("No consumers found for region {}, using fallback assignment", region);
                }
                
                distributePartitionsAmongConsumers(partitions, regionConsumers, assignment);
            }
        }
        
        logAssignmentSummary("Geographic Affinity", assignment);
        return assignment;
    }
    
    /**
     * Load-Balanced Assignment with Processing Weight Consideration
     * 
     * Real-world use case: High-frequency trading system during market hours
     * 
     * Scenario: Some partitions handle more complex financial instruments
     * that require intensive processing. Simple round-robin would overload
     * consumers handling complex partitions.
     * 
     * Rebalancing triggers:
     * - Market volatility changing processing complexity
     * - New trading instruments requiring different processing power
     * - Consumer hardware upgrades changing capacity
     */
    private Map<String, List<TopicPartition>> loadBalancedAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing load-balanced assignment for optimal throughput distribution");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, Double> consumerLoad = new HashMap<>();
        
        // Initialize
        subscriptions.keySet().forEach(consumer -> {
            assignment.put(consumer, new ArrayList<>());
            consumerLoad.put(consumer, 0.0);
        });
        
        // Create all partitions with estimated load weights
        List<WeightedPartition> weightedPartitions = new ArrayList<>();
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            for (int partition = 0; partition < partitionCount; partition++) {
                double weight = estimatePartitionWeight(topic, partition, partitionCount);
                weightedPartitions.add(new WeightedPartition(
                    new TopicPartition(topic, partition), weight));
            }
        }
        
        // Sort partitions by weight (heaviest first) for better load balancing
        weightedPartitions.sort((a, b) -> Double.compare(b.weight, a.weight));
        
        // Assign each partition to the least loaded consumer
        for (WeightedPartition wp : weightedPartitions) {
            String leastLoadedConsumer = consumerLoad.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(subscriptions.keySet().iterator().next());
            
            assignment.get(leastLoadedConsumer).add(wp.partition);
            consumerLoad.put(leastLoadedConsumer, 
                consumerLoad.get(leastLoadedConsumer) + wp.weight);
        }
        
        logAssignmentSummary("Load Balanced", assignment);
        logLoadDistribution(consumerLoad);
        return assignment;
    }
    
    /**
     * Sticky Priority Assignment
     * 
     * Real-world use case: Critical monitoring system with alert processing
     * 
     * Scenario: System has critical vs non-critical consumers where reassignments
     * are expensive (lose in-memory state) but priority must be respected.
     * Critical alerts must be processed by high-priority consumers.
     * 
     * Rebalancing triggers:
     * - Critical system alerts requiring priority escalation
     * - Consumer priority changes due to maintenance
     * - Alert severity classification changes
     */
    private Map<String, List<TopicPartition>> stickyPriorityAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing sticky priority assignment to minimize disruption while respecting priorities");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, Integer> consumerPriorities = extractConsumerPriorities(subscriptions);
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        // Get previous assignments from user data (if available)
        Map<String, List<TopicPartition>> previousAssignments = extractPreviousAssignments(subscriptions);
        
        // Create list of all partitions
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            for (int partition = 0; partition < topicEntry.getValue(); partition++) {
                allPartitions.add(new TopicPartition(topic, partition));
            }
        }
        
        // Sort consumers by priority (highest first)
        List<String> sortedConsumers = consumerPriorities.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
        
        Set<TopicPartition> assignedPartitions = new HashSet<>();
        
        // Phase 1: Try to maintain previous assignments for high-priority consumers
        for (String consumer : sortedConsumers) {
            List<TopicPartition> previousPartitions = previousAssignments.getOrDefault(consumer, new ArrayList<>());
            for (TopicPartition partition : previousPartitions) {
                if (allPartitions.contains(partition) && !assignedPartitions.contains(partition)) {
                    assignment.get(consumer).add(partition);
                    assignedPartitions.add(partition);
                }
            }
        }
        
        // Phase 2: Assign remaining partitions by priority
        List<TopicPartition> unassignedPartitions = allPartitions.stream()
            .filter(p -> !assignedPartitions.contains(p))
            .collect(Collectors.toList());
        
        distributePartitionsAmongConsumers(unassignedPartitions, sortedConsumers, assignment);
        
        logAssignmentSummary("Sticky Priority", assignment);
        return assignment;
    }
    
    /**
     * Capacity-Aware Assignment
     * 
     * Real-world use case: IoT data processing with heterogeneous infrastructure
     * 
     * Scenario: IoT sensors generate massive data streams. Some consumers run on
     * powerful cloud instances, others on edge devices with limited resources.
     * Assignment must respect processing capacity.
     * 
     * Rebalancing triggers:
     * - Infrastructure scaling (adding more powerful instances)
     * - Edge device failures requiring redistribution
     * - Data volume spikes exceeding current capacity
     */
    private Map<String, List<TopicPartition>> capacityAwareAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing capacity-aware assignment for optimal resource utilization");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, Double> consumerCapacities = extractConsumerCapacities(subscriptions);
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        // Calculate total capacity
        double totalCapacity = consumerCapacities.values().stream()
            .mapToDouble(Double::doubleValue)
            .sum();
        
        // Create all partitions
        List<TopicPartition> allPartitions = new ArrayList<>();
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            for (int partition = 0; partition < topicEntry.getValue(); partition++) {
                allPartitions.add(new TopicPartition(topic, partition));
            }
        }
        
        int totalPartitions = allPartitions.size();
        
        // Calculate target partition count for each consumer based on capacity
        Map<String, Integer> targetPartitionCounts = new HashMap<>();
        for (Map.Entry<String, Double> capacityEntry : consumerCapacities.entrySet()) {
            String consumer = capacityEntry.getKey();
            double capacity = capacityEntry.getValue();
            int targetCount = (int) Math.round((capacity / totalCapacity) * totalPartitions);
            targetPartitionCounts.put(consumer, Math.max(1, targetCount)); // Ensure at least 1
        }
        
        // Adjust for rounding errors
        int assignedTotal = targetPartitionCounts.values().stream().mapToInt(Integer::intValue).sum();
        if (assignedTotal != totalPartitions) {
            // Give extra partitions to highest capacity consumer
            String highestCapacityConsumer = consumerCapacities.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(subscriptions.keySet().iterator().next());
            
            targetPartitionCounts.put(highestCapacityConsumer,
                targetPartitionCounts.get(highestCapacityConsumer) + (totalPartitions - assignedTotal));
        }
        
        // Assign partitions according to calculated targets
        Collections.shuffle(allPartitions); // Randomize for fairness
        int partitionIndex = 0;
        
        for (Map.Entry<String, Integer> targetEntry : targetPartitionCounts.entrySet()) {
            String consumer = targetEntry.getKey();
            int targetCount = targetEntry.getValue();
            
            for (int i = 0; i < targetCount && partitionIndex < allPartitions.size(); i++) {
                assignment.get(consumer).add(allPartitions.get(partitionIndex++));
            }
        }
        
        logAssignmentSummary("Capacity Aware", assignment);
        logCapacityDistribution(consumerCapacities, targetPartitionCounts);
        return assignment;
    }
    
    /**
     * Cross-Zone Resilient Assignment
     * 
     * Real-world use case: Mission-critical payment processing system
     * 
     * Scenario: Payment transactions must continue processing even if an entire
     * availability zone fails. Each critical partition needs backup processing
     * capability in different zones.
     * 
     * Rebalancing triggers:
     * - Zone failure requiring immediate failover
     * - Planned zone maintenance requiring gradual migration
     * - Network partition isolating zones
     */
    private Map<String, List<TopicPartition>> crossZoneResilientAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        logger.info("Executing cross-zone resilient assignment for fault tolerance");
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        Map<String, String> consumerZones = extractConsumerZones(subscriptions);
        
        // Group consumers by zone
        Map<String, List<String>> zoneConsumers = new HashMap<>();
        for (Map.Entry<String, String> entry : consumerZones.entrySet()) {
            String consumer = entry.getKey();
            String zone = entry.getValue();
            zoneConsumers.computeIfAbsent(zone, k -> new ArrayList<>()).add(consumer);
        }
        
        // Initialize assignments
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            int partitionCount = topicEntry.getValue();
            
            List<String> availableZones = new ArrayList<>(zoneConsumers.keySet());
            Collections.shuffle(availableZones); // Randomize zone order
            
            for (int partition = 0; partition < partitionCount; partition++) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                
                // Assign to a consumer in a zone that hasn't processed this partition type recently
                String selectedZone = availableZones.get(partition % availableZones.size());
                List<String> zoneConsumerList = zoneConsumers.get(selectedZone);
                
                if (!zoneConsumerList.isEmpty()) {
                    // Select least loaded consumer in the zone
                    String selectedConsumer = zoneConsumerList.stream()
                        .min(Comparator.comparing(c -> assignment.get(c).size()))
                        .orElse(zoneConsumerList.get(0));
                    
                    assignment.get(selectedConsumer).add(topicPartition);
                }
            }
        }
        
        logAssignmentSummary("Cross-Zone Resilient", assignment);
        logZoneDistribution(consumerZones, assignment);
        return assignment;
    }

    /**
     * Logs the impact of rebalancing for monitoring and debugging
     */
    private void logRebalancingImpact(RebalancingTrigger trigger, AssignmentStrategy strategy, 
                                    Map<String, List<TopicPartition>> assignment) {
        
        logger.info("=== REBALANCING IMPACT ANALYSIS ===");
        logger.info("Trigger: {}", trigger);
        logger.info("Strategy: {}", strategy);
        
        int totalPartitions = assignment.values().stream()
            .mapToInt(List::size)
            .sum();
        
        logger.info("Total partitions assigned: {}", totalPartitions);
        logger.info("Consumer count: {}", assignment.size());
        
        // Calculate load distribution
        double avgPartitionsPerConsumer = (double) totalPartitions / assignment.size();
        double maxDeviation = assignment.values().stream()
            .mapToDouble(partitions -> Math.abs(partitions.size() - avgPartitionsPerConsumer))
            .max()
            .orElse(0);
        
        logger.info("Average partitions per consumer: {:.2f}", avgPartitionsPerConsumer);
        logger.info("Maximum deviation from average: {:.2f}", maxDeviation);
        
        // Real-world impact scenarios
        switch (trigger) {
            case CONSUMER_JOIN:
                logger.info("SCALING UP: New consumer capacity available, reducing load per consumer");
                break;
            case CONSUMER_LEAVE:
                logger.info("SCALING DOWN: Consumer failure/removal, redistributing load");
                break;
            case ZONE_FAILURE:
                logger.info("DISASTER RECOVERY: Zone failure detected, emergency rebalancing");
                break;
            case SLA_VIOLATION:
                logger.info("SLA BREACH: Priority reassignment to meet service commitments");
                break;
            case MAINTENANCE_WINDOW:
                logger.info("PLANNED MAINTENANCE: Gradual migration to reduce disruption");
                break;
            default:
                logger.info("ROUTINE REBALANCING: Standard partition assignment");
        }
        
        logger.info("=== END REBALANCING ANALYSIS ===");
    }

    // Extract methods for consumer metadata
    
    private Map<String, String> extractConsumerRoles(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, String> roles = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String role = extractUserDataValue(entry.getValue(), "role", "primary");
            roles.put(consumer, role);
        }
        return roles;
    }
    
    private Map<String, String> extractConsumerTenants(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, String> tenants = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String tenant = extractUserDataValue(entry.getValue(), "tenant", "shared");
            tenants.put(consumer, tenant);
        }
        return tenants;
    }
    
    private Map<String, String> extractConsumerSlaTiers(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, String> slaTiers = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String slaTier = extractUserDataValue(entry.getValue(), "sla.tier", "BRONZE");
            slaTiers.put(consumer, slaTier.toUpperCase());
        }
        return slaTiers;
    }
    
    // Helper methods for assignment logic
    
    private String findStandbyInDifferentZone(List<String> standbyConsumers, String primaryZone, 
                                            Map<String, String> consumerZones) {
        return standbyConsumers.stream()
            .filter(consumer -> !primaryZone.equals(consumerZones.get(consumer)))
            .findFirst()
            .orElse(standbyConsumers.isEmpty() ? null : standbyConsumers.get(0));
    }
    
    private String extractTenantFromTopic(String topic) {
        // Extract tenant from topic name patterns like: tenant1-events, tenant2-logs
        if (topic.contains("-")) {
            return topic.split("-")[0];
        }
        return "shared"; // Default to shared tenant
    }
    
    private String inferPartitionSla(String topic, int partition, int partitionCount) {
        // Simple heuristic: first partitions handle high-priority data
        // In practice, this would use historical metrics
        if (partition < partitionCount * 0.2) {
            return "GOLD";
        } else if (partition < partitionCount * 0.6) {
            return "SILVER";
        } else {
            return "BRONZE";
        }
    }
    
    private boolean isHigherSla(String sla1, String sla2) {
        Map<String, Integer> slaRank = Map.of("GOLD", 3, "SILVER", 2, "BRONZE", 1);
        return slaRank.getOrDefault(sla1, 0) > slaRank.getOrDefault(sla2, 0);
    }
    
    // Logging methods for different distribution types
    
    private void logTenantDistribution(Map<String, String> consumerTenants, 
                                     Map<String, List<TopicPartition>> assignment) {
        logger.info("Tenant distribution:");
        Map<String, Integer> tenantPartitionCounts = new HashMap<>();
        
        for (Map.Entry<String, List<TopicPartition>> entry : assignment.entrySet()) {
            String consumer = entry.getKey();
            String tenant = consumerTenants.get(consumer);
            int partitionCount = entry.getValue().size();
            tenantPartitionCounts.put(tenant, tenantPartitionCounts.getOrDefault(tenant, 0) + partitionCount);
        }
        
        for (Map.Entry<String, Integer> entry : tenantPartitionCounts.entrySet()) {
            logger.info("  Tenant {}: {} partitions", entry.getKey(), entry.getValue());
        }
    }
    
    private void logSlaDistribution(Map<String, String> consumerSlaTiers, 
                                  Map<String, List<TopicPartition>> assignment) {
        logger.info("SLA tier distribution:");
        Map<String, Integer> slaPartitionCounts = new HashMap<>();
        
        for (Map.Entry<String, List<TopicPartition>> entry : assignment.entrySet()) {
            String consumer = entry.getKey();
            String slaTier = consumerSlaTiers.get(consumer);
            int partitionCount = entry.getValue().size();
            slaPartitionCounts.put(slaTier, slaPartitionCounts.getOrDefault(slaTier, 0) + partitionCount);
        }
        
        for (Map.Entry<String, Integer> entry : slaPartitionCounts.entrySet()) {
            logger.info("  SLA Tier {}: {} partitions", entry.getKey(), entry.getValue());
        }
    }
    
    // Helper methods and utility classes
    
    private static class WeightedPartition {
        final TopicPartition partition;
        final double weight;
        
        WeightedPartition(TopicPartition partition, double weight) {
            this.partition = partition;
            this.weight = weight;
        }
    }
    
    private AssignmentStrategy determineAssignmentStrategy(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        // Look for strategy in any consumer's user data
        for (ConsumerPartitionAssignor.Subscription subscription : subscriptions.values()) {
            if (subscription.userData() != null) {
                String userData = byteBufferToString(subscription.userData());
                String searchKey = "strategy=";
                if (userData.contains(searchKey)) {
                    String strategy = userData.split(searchKey)[1].split(";")[0];
                    try {
                        return AssignmentStrategy.valueOf(strategy.toUpperCase());
                    } catch (IllegalArgumentException e) {
                        logger.warn("Unknown assignment strategy: {}", strategy);
                    }
                }
            }
        }
        return AssignmentStrategy.LOAD_BALANCED; // Default
    }
    
    private Map<String, String> extractConsumerRegions(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, String> regions = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String region = extractUserDataValue(entry.getValue(), "region", "default");
            regions.put(consumer, region);
        }
        return regions;
    }
    
    private Map<String, String> extractConsumerZones(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, String> zones = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String zone = extractUserDataValue(entry.getValue(), "zone", "zone-1");
            zones.put(consumer, zone);
        }
        return zones;
    }
    
    private Map<String, Integer> extractConsumerPriorities(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, Integer> priorities = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String priorityStr = extractUserDataValue(entry.getValue(), "priority", "1");
            try {
                priorities.put(consumer, Integer.parseInt(priorityStr));
            } catch (NumberFormatException e) {
                priorities.put(consumer, 1);
            }
        }
        return priorities;
    }
    
    private Map<String, Double> extractConsumerCapacities(Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        Map<String, Double> capacities = new HashMap<>();
        for (Map.Entry<String, ConsumerPartitionAssignor.Subscription> entry : subscriptions.entrySet()) {
            String consumer = entry.getKey();
            String capacityStr = extractUserDataValue(entry.getValue(), "capacity", "1.0");
            try {
                capacities.put(consumer, Double.parseDouble(capacityStr));
            } catch (NumberFormatException e) {
                capacities.put(consumer, 1.0);
            }
        }
        return capacities;
    }
    
    private String extractUserDataValue(ConsumerPartitionAssignor.Subscription subscription, String key, String defaultValue) {
        if (subscription.userData() != null) {
            String userData = byteBufferToString(subscription.userData());
            String searchKey = key + "=";
            if (userData.contains(searchKey)) {
                String value = userData.split(searchKey)[1].split(";")[0];
                return value.isEmpty() ? defaultValue : value;
            }
        }
        return defaultValue;
    }
    
    private Map<String, List<TopicPartition>> extractPreviousAssignments(
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        // In a real implementation, this would extract previous assignments from consumer metadata
        return new HashMap<>();
    }
    
    private Map<String, List<TopicPartition>> groupPartitionsByRegion(String topic, int partitionCount) {
        Map<String, List<TopicPartition>> regionPartitions = new HashMap<>();
        
        // Simple strategy: distribute partitions across regions based on partition number
        String[] regions = {"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"};
        
        for (int partition = 0; partition < partitionCount; partition++) {
            String region = regions[partition % regions.length];
            regionPartitions.computeIfAbsent(region, k -> new ArrayList<>())
                .add(new TopicPartition(topic, partition));
        }
        
        return regionPartitions;
    }
    
    private double estimatePartitionWeight(String topic, int partition, int totalPartitions) {
        // Simple heuristic: lower numbered partitions tend to be busier
        // In practice, this would use historical metrics
        double baseWeight = 1.0;
        double skewFactor = 1.0 - ((double) partition / totalPartitions) * 0.5;
        return baseWeight * skewFactor;
    }
    
    private void distributePartitionsAmongConsumers(List<TopicPartition> partitions, 
                                                  List<String> consumers,
                                                  Map<String, List<TopicPartition>> assignment) {
        if (consumers.isEmpty()) return;
        
        for (int i = 0; i < partitions.size(); i++) {
            String consumer = consumers.get(i % consumers.size());
            assignment.get(consumer).add(partitions.get(i));
        }
    }
    
    private Map<String, List<TopicPartition>> roundRobinAssignment(
            Map<String, Integer> partitionsPerTopic,
            Map<String, ConsumerPartitionAssignor.Subscription> subscriptions) {
        
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        List<String> consumers = new ArrayList<>(subscriptions.keySet());
        
        subscriptions.keySet().forEach(consumer -> assignment.put(consumer, new ArrayList<>()));
        
        int consumerIndex = 0;
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            for (int partition = 0; partition < topicEntry.getValue(); partition++) {
                String consumer = consumers.get(consumerIndex % consumers.size());
                assignment.get(consumer).add(new TopicPartition(topic, partition));
                consumerIndex++;
            }
        }
        
        return assignment;
    }
    
    private void logAssignmentSummary(String strategyName, Map<String, List<TopicPartition>> assignment) {
        logger.info("{} assignment summary:", strategyName);
        for (Map.Entry<String, List<TopicPartition>> entry : assignment.entrySet()) {
            logger.info("  Consumer {}: {} partitions", entry.getKey(), entry.getValue().size());
        }
    }
    
    private void logLoadDistribution(Map<String, Double> consumerLoad) {
        logger.info("Load distribution:");
        for (Map.Entry<String, Double> entry : consumerLoad.entrySet()) {
            logger.info("  Consumer {}: load {}", entry.getKey(), String.format("%.2f", entry.getValue()));
        }
    }
    
    private void logCapacityDistribution(Map<String, Double> capacities, Map<String, Integer> assignments) {
        logger.info("Capacity-based distribution:");
        for (String consumer : capacities.keySet()) {
            logger.info("  Consumer {}: capacity {}, assigned {} partitions", 
                consumer, capacities.get(consumer), assignments.get(consumer));
        }
    }
    
    private void logZoneDistribution(Map<String, String> consumerZones, Map<String, List<TopicPartition>> assignment) {
        Map<String, Integer> zonePartitionCounts = new HashMap<>();
        
        for (Map.Entry<String, List<TopicPartition>> entry : assignment.entrySet()) {
            String consumer = entry.getKey();
            String zone = consumerZones.get(consumer);
            int partitionCount = entry.getValue().size();
            zonePartitionCounts.put(zone, zonePartitionCounts.getOrDefault(zone, 0) + partitionCount);
        }
        
        logger.info("Zone distribution:");
        for (Map.Entry<String, Integer> entry : zonePartitionCounts.entrySet()) {
            logger.info("  Zone {}: {} partitions", entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * Helper method to convert ByteBuffer to String for parsing userData
     */
    private String byteBufferToString(java.nio.ByteBuffer byteBuffer) {
        if (byteBuffer == null) {
            return "";
        }
        // Create a byte array from ByteBuffer
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        byteBuffer.rewind(); // Reset position to allow reuse
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }
}
