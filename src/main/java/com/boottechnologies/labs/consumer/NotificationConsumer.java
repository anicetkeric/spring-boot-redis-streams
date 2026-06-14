package com.boottechnologies.labs.consumer;

import com.boottechnologies.labs.config.StreamProperties;
import com.boottechnologies.labs.event.OrderEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final StreamProperties props;

    public NotificationConsumer(StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper,
                                StreamProperties props) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.props = props;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String orderId = message.getValue().get("orderId");
        String payload = message.getValue().get("payload");

        log.debug("[notification-group] Received message: id={} orderId={}", message.getId(), orderId);

        try {
            OrderEvent event = objectMapper.readValue(payload, OrderEvent.class);
            sendOrderConfirmationNotification(event);
            acknowledge(message);

        } catch (Exception e) {
            log.error("[notification-group] Failed to process message: id={} orderId={} error={}",
                    message.getId(), orderId, e.getMessage());
            // For notifications, we acknowledge even on failure to avoid blocking the stream
            // and log for manual retry via the monitoring pipeline
            acknowledge(message);
        }
    }

    private void sendOrderConfirmationNotification(OrderEvent event) {
        log.info("[notification-group] Sending order confirmation: orderId={} customerId={} amount={}",
                event.orderId(), event.customerId(), event.totalAmount());
        // In production: call email/SMS/push notification service
    }

    private void acknowledge(MapRecord<String, String, String> message) {
        redisTemplate.opsForStream().acknowledge(props.notificationGroup(), message);
        log.debug("[notification-group] ACK: id={}", message.getId());
    }
}
