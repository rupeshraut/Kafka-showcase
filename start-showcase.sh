#!/bin/bash

# Kafka Showcase - Start Script
# This script starts the Kafka ecosystem and runs the demonstration

echo "🚀 Kafka Advanced Showcase"
echo "=========================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Function to check if services are healthy
check_services() {
    echo "🔍 Checking service health..."
    
    # Check Kafka
    if ! curl -s http://localhost:9092 > /dev/null 2>&1; then
        echo "⚠️  Kafka is not yet ready. Waiting..."
        return 1
    fi
    
    # Check Schema Registry
    if ! curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
        echo "⚠️  Schema Registry is not yet ready. Waiting..."
        return 1
    fi
    
    echo "✅ Services are healthy!"
    return 0
}

# Start Docker Compose services
echo "🐳 Starting Kafka ecosystem..."
cd docker
docker-compose up -d

echo "⏳ Waiting for services to be ready (this may take a few minutes)..."
for i in {1..30}; do
    if check_services; then
        break
    fi
    sleep 10
    echo "   Attempt $i/30..."
done

if ! check_services; then
    echo "❌ Services failed to start properly. Check docker-compose logs."
    exit 1
fi

# Go back to project root
cd ..

echo ""
echo "🎯 Kafka Showcase is ready!"
echo ""
echo "📊 Access the following web interfaces:"
echo "   • Kafka UI:        http://localhost:8080"
echo "   • Control Center:  http://localhost:9021"
echo "   • Schema Registry: http://localhost:8081"
echo "   • Grafana:         http://localhost:3000 (admin/admin)"
echo "   • Prometheus:      http://localhost:9090"
echo ""
echo "🏃 Running the application..."
echo ""

# Run the application
./gradlew run
