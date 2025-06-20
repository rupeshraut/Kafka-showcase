#!/bin/bash

# Kafka Showcase - Stop Script
# This script stops all services and cleans up

echo "🛑 Stopping Kafka Showcase"
echo "========================="

cd docker

# Stop all services
echo "🐳 Stopping Docker Compose services..."
docker-compose down

# Optional: Remove volumes (uncomment if you want to clean all data)
# echo "🗑️  Removing volumes..."
# docker-compose down -v

echo "✅ Kafka Showcase stopped successfully!"
echo ""
echo "To remove all data volumes, run:"
echo "   cd docker && docker-compose down -v"
