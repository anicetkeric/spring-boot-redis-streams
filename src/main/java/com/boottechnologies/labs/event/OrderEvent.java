package com.boottechnologies.labs.event;

import com.boottechnologies.labs.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        List<String> itemIds,
        BigDecimal totalAmount,
        OrderStatus status,
        Instant occurredAt,
        int retryCount
) {

    public static OrderEvent of(String orderId, String customerId, List<String> itemIds, BigDecimal totalAmount) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                itemIds,
                totalAmount,
                OrderStatus.PENDING,
                Instant.now(),
                0
        );
    }

    public OrderEvent withRetryCount(int count) {
        return new OrderEvent(eventId, orderId, customerId, itemIds, totalAmount, status, occurredAt, count);
    }
}
