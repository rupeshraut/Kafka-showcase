package com.example.kafka.partitioning;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Advanced custom partitioner demonstrating multiple partitioning strategies
 * for different real-world use cases:
 * 
 * 1. Geographic partitioning for global applications
 * 2. Temporal partitioning for time-series data
 * 3. User tier-based partitioning for SLA guarantees
 * 4. Load-balanced partitioning with hot partition avoidance
 * 5. Consistent hashing for session affinity
 */
public class AdvancedPartitioner implements Partitioner {
    
    private static final Logger logger = LoggerFactory.getLogger(AdvancedPartitioner.class);
    
    // Configuration keys
    private static final String STRATEGY_CONFIG = "partitioning.strategy";
    private static final String GEOGRAPHIC_REGIONS_CONFIG = "partitioning.geographic.regions";
    private static final String TIME_WINDOW_MINUTES_CONFIG = "partitioning.time.window.minutes";
    private static final String USER_TIER_PARTITIONS_CONFIG = "partitioning.user.tier.partitions";
    private static final String HOT_PARTITION_THRESHOLD_CONFIG = "partitioning.hot.threshold";
    
    // Partitioning strategies
    public enum PartitioningStrategy {
        GEOGRAPHIC,      // Route by geographic region
        TEMPORAL,        // Route by time windows
        USER_TIER,       // Route by user subscription tier
        LOAD_BALANCED,   // Avoid hot partitions
        CONSISTENT_HASH, // Session affinity with consistent hashing
        MODULUS,         // Simple modulus-based partitioning for even distribution
        HYBRID          // Combination of multiple strategies
    }
    
    private PartitioningStrategy strategy;
    private Map<String, Object> configs;
    private final Map<Integer, Long> partitionLoadCounter = new ConcurrentHashMap<>();
    private final MessageDigest md5;
    
    public AdvancedPartitioner() {
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize MD5", e);
        }
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = new ConcurrentHashMap<>();
        this.configs.putAll(configs);
        
        Object strategyObj = configs.get(STRATEGY_CONFIG);
        String strategyName = strategyObj != null ? strategyObj.toString() : "LOAD_BALANCED";
        this.strategy = PartitioningStrategy.valueOf(strategyName.toUpperCase());
        
        logger.info("AdvancedPartitioner configured with strategy: {}", strategy);
    }
    
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, 
                        byte[] valueBytes, Cluster cluster) {
        
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        if (key == null) {
            return roundRobinPartition(numPartitions);
        }
        
        String keyString = key.toString();
        
        switch (strategy) {
            case GEOGRAPHIC:
                return geographicPartition(keyString, numPartitions);
            case TEMPORAL:
                return temporalPartition(keyString, numPartitions);
            case USER_TIER:
                return userTierPartition(keyString, value, numPartitions);
            case LOAD_BALANCED:
                return loadBalancedPartition(keyString, numPartitions);
            case CONSISTENT_HASH:
                return consistentHashPartition(keyString, numPartitions);
            case MODULUS:
                return modulusPartition(keyString, numPartitions);
            case HYBRID:
                return hybridPartition(keyString, value, numPartitions);
            default:
                return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }
    
    /**
     * Geographic partitioning based on region codes
     * Use case: Global applications with regional data locality requirements
     * 
     * Example key format: "user123:region:us-east-1"
     */
    private int geographicPartition(String key, int numPartitions) {
        String region = extractRegionFromKey(key);
        
        // Define region to partition mapping
        Map<String, Integer> regionPartitions = Map.of(
            "us-east-1", 0,
            "us-west-2", 1,
            "eu-west-1", 2,
            "ap-south-1", 3,
            "default", numPartitions - 1
        );
        
        int basePartition = regionPartitions.getOrDefault(region, regionPartitions.get("default"));
        
        // If we have more partitions than regions, use round-robin within region
        if (numPartitions > regionPartitions.size()) {
            int partitionsPerRegion = numPartitions / regionPartitions.size();
            int keyHash = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)));
            int offset = keyHash % partitionsPerRegion;
            return (basePartition * partitionsPerRegion + offset) % numPartitions;
        }
        
        return basePartition % numPartitions;
    }
    
    /**
     * Temporal partitioning for time-series data
     * Use case: Time-series analytics, log aggregation with time-based processing
     * 
     * Example key format: "sensor123:2023-06-20T10:30:00Z"
     */
    private int temporalPartition(String key, int numPartitions) {
        long timestamp = extractTimestampFromKey(key);
        
        // Get time window in minutes (default 60 minutes)
        int timeWindowMinutes = (Integer) configs.getOrDefault(TIME_WINDOW_MINUTES_CONFIG, 60);
        
        // Calculate time slot
        long timeSlot = (timestamp / (1000L * 60 * timeWindowMinutes));
        
        // Distribute time slots across partitions
        return (int) (timeSlot % numPartitions);
    }
    
    /**
     * User tier-based partitioning for SLA guarantees
     * Use case: Multi-tenant systems with different service levels
     * 
     * Premium users get dedicated partitions for guaranteed performance
     */
    private int userTierPartition(String key, Object value, int numPartitions) {
        String userTier = extractUserTierFromValue(value);
        
        switch (userTier.toLowerCase()) {
            case "premium":
                // Premium users get first 20% of partitions
                int premiumPartitions = Math.max(1, numPartitions / 5);
                int keyHash = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)));
                return keyHash % premiumPartitions;
                
            case "business":
                // Business users get next 30% of partitions
                int businessStart = Math.max(1, numPartitions / 5);
                int businessPartitions = Math.max(1, (numPartitions * 3) / 10);
                int businessHash = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)));
                return businessStart + (businessHash % businessPartitions);
                
            case "standard":
            default:
                // Standard users get remaining 50% of partitions
                int standardStart = Math.max(1, numPartitions / 2);
                int standardPartitions = numPartitions - standardStart;
                int standardHash = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8)));
                return standardStart + (standardHash % Math.max(1, standardPartitions));
        }
    }
    
    /**
     * Load-balanced partitioning with hot partition avoidance
     * Use case: High-throughput systems with uneven key distributions
     */
    private int loadBalancedPartition(String key, int numPartitions) {
        // Get hot partition threshold (default 1000 messages)
        long hotThreshold = (Long) configs.getOrDefault(HOT_PARTITION_THRESHOLD_CONFIG, 1000L);
        
        // Calculate default partition
        int defaultPartition = Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % numPartitions;
        
        // Check if default partition is too hot
        long currentLoad = partitionLoadCounter.getOrDefault(defaultPartition, 0L);
        
        if (currentLoad > hotThreshold) {
            // Find least loaded partition
            int leastLoadedPartition = findLeastLoadedPartition(numPartitions);
            partitionLoadCounter.put(leastLoadedPartition, 
                partitionLoadCounter.getOrDefault(leastLoadedPartition, 0L) + 1);
            
            logger.debug("Redirected from hot partition {} (load: {}) to partition {} for key: {}", 
                defaultPartition, currentLoad, leastLoadedPartition, key);
            
            return leastLoadedPartition;
        }
        
        // Update load counter
        partitionLoadCounter.put(defaultPartition, currentLoad + 1);
        return defaultPartition;
    }
    
    /**
     * Consistent hashing for session affinity
     * Use case: Stateful stream processing, session-aware applications
     */
    private int consistentHashPartition(String key, int numPartitions) {
        // Extract session ID or use entire key for session affinity
        String sessionKey = extractSessionFromKey(key);
        
        synchronized (md5) {
            md5.reset();
            byte[] hash = md5.digest(sessionKey.getBytes(StandardCharsets.UTF_8));
            
            // Use first 4 bytes of MD5 hash
            int hashCode = ((hash[0] & 0xFF) << 24) |
                          ((hash[1] & 0xFF) << 16) |
                          ((hash[2] & 0xFF) << 8) |
                          (hash[3] & 0xFF);
            
            return Utils.toPositive(hashCode) % numPartitions;
        }
    }
    
    /**
     * Simple modulus-based partitioning for even distribution
     * Use case: Simple, predictable partitioning when even distribution is the primary goal
     * 
     * This method provides:
     * - Deterministic partition assignment (same key always goes to same partition)
     * - Even distribution across partitions for sequential or numeric keys
     * - Simple and fast computation with minimal overhead
     * - Useful for numeric IDs, sequential keys, or when simplicity is preferred
     */
    private int modulusPartition(String key, int numPartitions) {
        logger.debug("Applying modulus partitioning for key: {}", key);
        
        // Try to extract numeric value from key for better distribution
        long numericKey = extractNumericValue(key);
        
        if (numericKey != -1) {
            // Use numeric value directly for modulus
            int partition = (int) (numericKey % numPartitions);
            logger.debug("Numeric key {} -> partition {}", numericKey, partition);
            return partition;
        } else {
            // Fall back to hash-based modulus for non-numeric keys
            int hashCode = key.hashCode();
            int partition = Utils.toPositive(hashCode) % numPartitions;
            logger.debug("Hash-based key {} (hash: {}) -> partition {}", key, hashCode, partition);
            return partition;
        }
    }
    
    /**
     * Extract numeric value from key for modulus partitioning
     * Supports various key formats like "user:123", "order-456", "789", etc.
     */
    private long extractNumericValue(String key) {
        try {
            // First try to parse the entire key as a number
            return Long.parseLong(key);
        } catch (NumberFormatException e) {
            // If not a pure number, try to extract numbers from the key
            String numericPart = key.replaceAll("[^0-9]", "");
            if (!numericPart.isEmpty()) {
                try {
                    return Long.parseLong(numericPart);
                } catch (NumberFormatException ex) {
                    // If the numeric part is too large, return -1
                    return -1;
                }
            }
            return -1;
        }
    }
    
    /**
     * Hybrid partitioning combining multiple strategies
     * Use case: Complex applications requiring different strategies for different data types
     */
    private int hybridPartition(String key, Object value, int numPartitions) {
        // Determine strategy based on key prefix or value type
        if (key.startsWith("geo:")) {
            return geographicPartition(key, numPartitions);
        } else if (key.startsWith("time:")) {
            return temporalPartition(key, numPartitions);
        } else if (key.startsWith("user:")) {
            return userTierPartition(key, value, numPartitions);
        } else if (key.startsWith("session:")) {
            return consistentHashPartition(key, numPartitions);
        } else if (key.startsWith("mod:") || key.matches("\\d+")) {
            // Use modulus for numeric keys or keys with "mod:" prefix
            return modulusPartition(key, numPartitions);
        } else {
            return loadBalancedPartition(key, numPartitions);
        }
    }
    
    /**
     * Round-robin partitioning for null keys
     */
    private int roundRobinPartition(int numPartitions) {
        return ThreadLocalRandom.current().nextInt(numPartitions);
    }
    
    // Helper methods for extracting information from keys and values
    
    private String extractRegionFromKey(String key) {
        String[] parts = key.split(":");
        for (int i = 0; i < parts.length - 1; i++) {
            if ("region".equals(parts[i]) && i + 1 < parts.length) {
                return parts[i + 1];
            }
        }
        return "default";
    }
    
    private long extractTimestampFromKey(String key) {
        try {
            String[] parts = key.split(":");
            String timestampStr = parts[parts.length - 1];
            // Try to parse as epoch millis or ISO timestamp
            if (timestampStr.matches("\\d+")) {
                return Long.parseLong(timestampStr);
            } else {
                // For ISO timestamps, use current time as fallback
                return System.currentTimeMillis();
            }
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
    
    private String extractUserTierFromValue(Object value) {
        if (value != null) {
            String valueStr = value.toString();
            if (valueStr.contains("\"tier\":\"premium\"") || valueStr.contains("premium")) {
                return "premium";
            } else if (valueStr.contains("\"tier\":\"business\"") || valueStr.contains("business")) {
                return "business";
            }
        }
        return "standard";
    }
    
    private String extractSessionFromKey(String key) {
        String[] parts = key.split(":");
        for (int i = 0; i < parts.length - 1; i++) {
            if ("session".equals(parts[i]) && i + 1 < parts.length) {
                return parts[i + 1];
            }
        }
        return key; // Use entire key if no session found
    }
    
    private int findLeastLoadedPartition(int numPartitions) {
        int leastLoaded = 0;
        long minLoad = partitionLoadCounter.getOrDefault(0, 0L);
        
        for (int i = 1; i < numPartitions; i++) {
            long load = partitionLoadCounter.getOrDefault(i, 0L);
            if (load < minLoad) {
                minLoad = load;
                leastLoaded = i;
            }
        }
        
        return leastLoaded;
    }
    
    @Override
    public void close() {
        logger.info("AdvancedPartitioner closing. Final partition loads: {}", partitionLoadCounter);
    }
}
