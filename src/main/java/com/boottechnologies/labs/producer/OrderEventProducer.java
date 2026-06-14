package com.boottechnologies.labs.producer;

import com.boottechnologies.labs.config.StreamProperties;
import com.boottechnologies.labs.event.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final StreamProperties props;

    public OrderEventProducer(StringRedisTemplate redisTemplate,
                              ObjectMapper objectMapper,
                              StreamProperties props) {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.props = props;
    }

    public RecordId publish(OrderEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);

            MapRecord<String, String, String> record = StreamRecords.string(Map.of(
                    "eventId", event.eventId(),
                    "orderId", event.orderId(),
                    "eventType", "ORDER_CREATED",
                    "payload", payload,
                    "retryCount", String.valueOf(event.retryCount())
            )).withStreamKey(props.streamKey());

            RecordId recordId = redisTemplate.opsForStream().add(record);

            trimStream();

            log.debug("Published ORDER_CREATED event: orderId={} recordId={}", event.orderId(), recordId);
            return recordId;

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderEvent: " + event.eventId(), e);
        }
    }

    public RecordId publishToDlq(OrderEvent event, String reason) {
        try {
            MapRecord<String, String, String> record = StreamRecords.string(Map.of(
                    "eventId", event.eventId(),
                    "orderId", event.orderId(),
                    "payload", objectMapper.writeValueAsString(event),
                    "reason", reason,
                    "retryCount", String.valueOf(event.retryCount())
            )).withStreamKey(props.dlqKey());

            RecordId recordId = redisTemplate.opsForStream().add(record);
            log.warn("Moved to DLQ: orderId={} reason={} recordId={}", event.orderId(), reason, recordId);
            return recordId;

        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize DLQ event: " + event.eventId(), e);
        }
    }

    private void trimStream() {
        redisTemplate.opsForStream().trim(props.streamKey(), props.maxLen(), true);
    }
}
