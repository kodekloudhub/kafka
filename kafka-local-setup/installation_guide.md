# Setting up Kafka and Kafka UI in GitHub Codespace

## Initial Setup

Create a Docker network to enable communication between containers:

```bash
docker network create kafka-net
```

## Running the Containers

### 1. Start Kafka Cluster
```bash
docker run --rm -d \
  --network kafka-net \
  -p 2181:2181 \
  -p 3030:3030 \
  -p 9092:9092 \
  -p 8081:8081 \
  -p 8082:8082 \
  -e ADV_HOST=kafka-cluster \
  --name kafka-cluster \
  lensesio/fast-data-dev
```

### 2. Start Kafka UI
```bash
docker run --rm -d \
  --network kafka-net \
  -p 7000:8080 \
  -e DYNAMIC_CONFIG_ENABLED=true \
  --name kafka-ui \
  provectuslabs/kafka-ui
```

## Configuration

When configuring the connection in Kafka UI, use:
- Bootstrap servers: `kafka-cluster:9092`

## Troubleshooting

If you encounter connectivity issues, try these commands:

### Check Kafka Cluster Logs
```bash
docker logs kafka-cluster
```

### Verify Port Exposure
```bash
docker ps
```

### Check Network Configuration
```bash
docker network inspect kafka-net
```

## Common Issues

The error `TimeoutException: Timed out waiting for a node assignment` typically occurs when:
- The Kafka UI cannot reach the Kafka cluster
- The advertised host is incorrectly configured
- Network connectivity issues between containers

Using the Docker network and container names instead of IP addresses resolves most connectivity issues in GitHub Codespace environments.