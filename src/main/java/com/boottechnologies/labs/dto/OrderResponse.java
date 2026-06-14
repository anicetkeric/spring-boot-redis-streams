package com.boottechnologies.labs.dto;

import com.boottechnologies.labs.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
        String orderId,
        String customerId,
        List<String> itemIds,
        BigDecimal totalAmount,
        OrderStatus status,
        String streamRecordId,
        Instant createdAt
) {}
