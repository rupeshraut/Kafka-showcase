<!-- Use this file to provide workspace-specific custom instructions to Copilot. For more details, visit https://code.visualstudio.com/docs/copilot/copilot-customization#_use-a-githubcopilotinstructionsmd-file -->

# Kafka Showcase - Copilot Instructions

This is a comprehensive Apache Kafka showcase project demonstrating advanced features and cutting-edge patterns.

## Project Context

This project showcases:
- Advanced Kafka producer and consumer patterns with exactly-once semantics
- Avro serialization with Schema Registry integration
- Kafka Streams for real-time processing
- Transactional processing patterns
- Comprehensive monitoring and observability

## Code Style Guidelines

### Java Code Style
- Use meaningful variable and method names that reflect Kafka concepts
- Include comprehensive JavaDoc comments for all public methods
- Follow the builder pattern for complex object creation (like Kafka configs)
- Use proper exception handling with specific Kafka exceptions
- Include logging at appropriate levels (DEBUG for detailed flow, INFO for business events)

### Kafka-Specific Patterns
- Always use exactly-once semantics where applicable
- Implement proper resource cleanup (close producers, consumers, streams)
- Use meaningful client IDs and consumer group IDs
- Include proper error handling for serialization/deserialization failures
- Implement graceful shutdown hooks for all components

### Configuration Management
- Centralize Kafka configurations in `KafkaConfigFactory`
- Use external configuration files for environment-specific settings
- Document all configuration parameters with their impact
- Provide sensible defaults for development environments

### Testing Guidelines
- Use Testcontainers for integration tests with real Kafka instances
- Mock external dependencies appropriately
- Test error scenarios and edge cases
- Include performance and load testing considerations

## Architecture Patterns

### Producer Patterns
- Implement asynchronous sending with callbacks
- Use proper batching for throughput optimization
- Include retry logic with exponential backoff
- Implement custom partitioning strategies when needed

### Consumer Patterns
- Use manual offset management for exactly-once processing
- Implement proper consumer group coordination
- Include consumer lag monitoring
- Handle rebalancing scenarios gracefully

### Streams Patterns
- Use appropriate serdes for data types
- Implement proper state store management
- Include windowing operations for time-based processing
- Design fault-tolerant topologies

## Documentation Standards

- Include comprehensive README files for each module
- Document all environment setup requirements
- Provide clear examples for each feature demonstrated
- Include troubleshooting guides for common issues

## Security Considerations

- Never hardcode sensitive configuration in source code
- Use proper authentication and authorization when available
- Implement proper SSL/TLS configuration for production scenarios
- Include security best practices in documentation

## Performance Guidelines

- Optimize for both throughput and latency based on use case
- Use appropriate compression algorithms
- Implement proper memory management
- Include performance tuning recommendations

## Monitoring and Observability

- Include comprehensive metrics collection
- Implement proper health checks
- Use structured logging for better observability
- Provide monitoring dashboard configurations
