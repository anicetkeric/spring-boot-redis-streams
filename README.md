# Redis Streams as a Message Broker — Spring Boot 4 Demo

Order event processing demo using Redis Streams with two consumer groups.

## Requirements

- Java 21
- Maven 3.9+
- Docker + Docker Compose

## Run

```bash
docker compose up -d
mvn spring-boot:run
```

## Test the API

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "customer-42",
    "itemIds": ["item-1", "item-2"],
    "totalAmount": 149.99
  }'
```

## Inspect Redis

Open Redis Insight at http://localhost:5540

```bash
# Stream length
redis-cli XLEN orders:events

# Consumer group info
redis-cli XINFO GROUPS orders:events

# Pending messages per group
redis-cli XPENDING orders:events inventory-group - + 10
redis-cli XPENDING orders:events notification-group - + 10
```

## Run Tests

```bash
mvn test
```
