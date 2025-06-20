package com.example.kafka;

import com.example.kafka.producers.AdvancedProducerDemo;
import com.example.kafka.consumers.AdvancedConsumerDemo;
import com.example.kafka.streams.StreamsDemo;
import com.example.kafka.transactions.TransactionDemo;
import com.example.kafka.partitioning.ConsumerRebalancingPatternsDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * Main application class for Kafka Showcase
 * 
 * This application demonstrates advanced Kafka features including:
 * - Exactly-once semantics with transactions
 * - Avro serialization with Schema Registry
 * - Kafka Streams processing
 * - Advanced producer and consumer patterns
 * - Error handling and monitoring
 */
public class App {
    
    private static final Logger logger = LoggerFactory.getLogger(App.class);
    
    public static void main(String[] args) {
        logger.info("=== Kafka Advanced Showcase ===");
        logger.info("This application demonstrates cutting-edge Kafka features");
        
        if (args.length > 0) {
            String mode = args[0].toLowerCase();
            runSpecificDemo(mode);
        } else {
            showInteractiveMenu();
        }
    }
    
    /**
     * Run a specific demo based on command line argument
     */
    private static void runSpecificDemo(String mode) {
        try {
            switch (mode) {
                case "producer":
                    logger.info("Running Advanced Producer Demo");
                    AdvancedProducerDemo.main(new String[]{});
                    break;
                case "consumer":
                    logger.info("Running Advanced Consumer Demo");
                    AdvancedConsumerDemo.main(new String[]{"kafka-showcase-consumer"});
                    break;
                case "streams":
                    logger.info("Running Kafka Streams Demo");
                    StreamsDemo.main(new String[]{});
                    break;
                case "transactions":
                    logger.info("Running Transaction Demo");
                    TransactionDemo.main(new String[]{});
                    break;
                case "rebalancing":
                    logger.info("Running Consumer Rebalancing Patterns Demo");
                    ConsumerRebalancingPatternsDemo.main(new String[]{});
                    break;
                default:
                    logger.error("Unknown mode: {}. Valid modes: producer, consumer, streams, transactions, rebalancing", mode);
                    showUsage();
            }
        } catch (Exception e) {
            logger.error("Error running demo: {}", mode, e);
        }
    }
    
    /**
     * Show interactive menu for demo selection
     */
    private static void showInteractiveMenu() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.println("\n=== Kafka Advanced Showcase - Interactive Menu ===");
                System.out.println("1. Advanced Producer Demo");
                System.out.println("2. Advanced Consumer Demo");
                System.out.println("3. Kafka Streams Demo");
                System.out.println("4. Transaction Demo");
                System.out.println("5. Consumer Rebalancing Patterns Demo");
                System.out.println("6. Show System Information");
                System.out.println("0. Exit");
                System.out.print("Select an option (0-6): ");
                
                try {
                    int choice = scanner.nextInt();
                
                switch (choice) {
                    case 1:
                        System.out.println("\nStarting Advanced Producer Demo...");
                        System.out.println("This demo shows exactly-once semantics, Avro serialization, and advanced error handling");
                        runInThread(() -> AdvancedProducerDemo.main(new String[]{}));
                        break;
                    case 2:
                        System.out.println("\nStarting Advanced Consumer Demo...");
                        System.out.println("This demo shows exactly-once processing, manual offset management, and consumer group coordination");
                        runInThread(() -> AdvancedConsumerDemo.main(new String[]{"interactive-consumer"}));
                        break;
                    case 3:
                        System.out.println("\nStarting Kafka Streams Demo...");
                        System.out.println("This demo shows stream processing, windowing, and real-time analytics");
                        runInThread(() -> StreamsDemo.main(new String[]{}));
                        break;
                    case 4:
                        System.out.println("\nStarting Transaction Demo...");
                        System.out.println("This demo shows consume-transform-produce pattern with transactions");
                        runInThread(() -> TransactionDemo.main(new String[]{}));
                        break;
                    case 5:
                        System.out.println("\nStarting Consumer Rebalancing Patterns Demo...");
                        System.out.println("This demo shows real-world rebalancing scenarios across different industries");
                        runInThread(() -> ConsumerRebalancingPatternsDemo.main(new String[]{}));
                        break;
                    case 6:
                        showSystemInformation();
                        break;
                    case 0:
                        System.out.println("Exiting Kafka Showcase. Goodbye!");
                        return;
                    default:
                        System.out.println("Invalid option. Please select 0-6.");
                }                } catch (Exception e) {
                    System.out.println("Invalid input. Please enter a number between 0-6.");
                    scanner.nextLine(); // Clear invalid input
                }
            }
        } // Close try-with-resources
    }
    
    /**
     * Run a demo in a separate thread
     */
    private static void runInThread(Runnable demo) {
        Thread demoThread = new Thread(demo);
        demoThread.setDaemon(true);
        demoThread.start();
        
        System.out.println("Demo started in background. Press Enter to return to menu...");
        try {
            System.in.read();
        } catch (Exception e) {
            // Ignore
        }
    }
    
    /**
     * Show system and configuration information
     */
    private static void showSystemInformation() {
        System.out.println("\n=== System Information ===");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("Java Vendor: " + System.getProperty("java.vendor"));
        System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        System.out.println("Available Processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Max Memory: " + (Runtime.getRuntime().maxMemory() / 1024 / 1024) + " MB");
        System.out.println("Free Memory: " + (Runtime.getRuntime().freeMemory() / 1024 / 1024) + " MB");
        
        System.out.println("\n=== Kafka Configuration ===");
        System.out.println("Bootstrap Servers: localhost:9092");
        System.out.println("Schema Registry: http://localhost:8081");
        System.out.println("Control Center: http://localhost:9021");
        System.out.println("Kafka UI: http://localhost:8080");
        System.out.println("Grafana: http://localhost:3000");
        
        System.out.println("\n=== Demo Features ===");
        System.out.println("✓ Exactly-once semantics");
        System.out.println("✓ Avro serialization with Schema Registry");
        System.out.println("✓ Kafka Streams processing");
        System.out.println("✓ Transactional processing");
        System.out.println("✓ Advanced error handling");
        System.out.println("✓ Monitoring and metrics");
        System.out.println("✓ Performance optimizations");
    }
    
    /**
     * Show usage information
     */
    private static void showUsage() {
        System.out.println("\nUsage: java -jar kafka-showcase.jar [mode]");
        System.out.println("Modes:");
        System.out.println("  producer     - Run advanced producer demo");
        System.out.println("  consumer     - Run advanced consumer demo");
        System.out.println("  streams      - Run Kafka Streams demo");
        System.out.println("  transactions - Run transaction demo");
        System.out.println("  rebalancing  - Run consumer rebalancing patterns demo");
        System.out.println("\nIf no mode is specified, interactive menu will be shown.");
    }
}
