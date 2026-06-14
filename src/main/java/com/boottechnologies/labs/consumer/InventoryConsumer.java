package com.boottechnologies.labs.consumer;

import com.boottechnologies.labs.config.StreamProperties;
import com.boottechnologies.labs.event.OrderEvent;
import com.boottechnologies.labs.producer.OrderEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class InventoryConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);
    private static final int MAX_RETRIES = 3;

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final StreamProperties props;
    private final OrderEventProducer producer;

    public InventoryConsumer(StringRedisTemplate redisTemplate,
                             ObjectMapper objectMapper,
                             StreamProperties props,
                             OrderEventProducer producer) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.props = props;
        this.producer = producer;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String orderId = message.getValue().get("orderId");
        String payload = message.getValue().get("payload");

        log.debug("[inventory-group] Received message: id={} orderId={}", message.getId(), orderId);

        try {
            OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
            processInventoryReservation(event);
            acknowledge(message);

        } catch (Exception e) {
            log.error("[inventory-group] Failed to process message: id={} orderId={} error={}",
                    message.getId(), orderId, e.getMessage());
            handleProcessingFailure(message, payload, e);
        }
    }

    private void processInventoryReservation(OrderEvent event) {
        log.info("[inventory-group] Reserving inventory for orderId={} items={} customerId={}",
                event.orderId(), event.itemIds(), event.customerId());
        // In production: call inventory service, update stock, persist reservation
    }

    private void acknowledge(MapRecord<String, String, String> message) {
        redisTemplate.opsForStream().acknowledge(props.inventoryGroup(), message);
        log.debug("[inventory-group] ACK: id={}", message.getId());
    }

    private void handleProcessingFailure(MapRecord<String, String, String> message, String payload, Exception cause) {
        int retryCount = parseRetryCount(message.getValue().get("retryCount"));

        if (retryCount >= MAX_RETRIES) {
            try {
                OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
                producer.publishToDlq(event, cause.getMessage());
            } catch (Exception ex) {
                log.error("[inventory-group] Failed to route to DLQ: id={}", message.getId(), ex);
            }
            acknowledge(message);
        } else {
            log.warn("[inventory-group] Will retry message id={} attempt={}/{}", message.getId(), retryCount + 1, MAX_RETRIES);
        }
    }

    @Scheduled(fixedDelayString = "${app.streams.orders.claim-min-idle-ms:30000}")
    public void reclaimAbandonedMessages() {
        try {
            var pendingMessages = redisTemplate.opsForStream()
                    .pending(props.streamKey(), props.inventoryGroup(), org.springframework.data.domain.Range.unbounded(), 10L);

            if (pendingMessages == null || pendingMessages.isEmpty()) {
                return;
            }

            pendingMessages.stream()
                    .filter(p -> p.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofMillis(props.claimMinIdleMs())) > 0)
                    .forEach(p -> log.warn("[inventory-group] Stale pending message detected: id={} idleFor={}",
                            p.getId(), p.getElapsedTimeSinceLastDelivery()));
        } catch (Exception e) {
            log.error("[inventory-group] Error checking pending messages: {}", e.getMessage());
        }
    }

    private int parseRetryCount(String value) {
        try {
            return value != null ? Integer.parseInt(value) : 0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
