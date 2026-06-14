package com.boottechnologies.labs.service;

import com.boottechnologies.labs.domain.OrderStatus;
import com.boottechnologies.labs.dto.CreateOrderRequest;
import com.boottechnologies.labs.dto.OrderResponse;
import com.boottechnologies.labs.event.OrderEvent;
import com.boottechnologies.labs.producer.OrderEventProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderEventProducer producer;

    public OrderService(OrderEventProducer producer) {
        this.producer = producer;
    }

    public OrderResponse createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        OrderEvent event = OrderEvent.of(
                orderId,
                request.customerId(),
                request.itemIds(),
                request.totalAmount()
        );

        RecordId recordId = producer.publish(event);

        log.info("Order created and published: orderId={} recordId={}", orderId, recordId);

        return new OrderResponse(
                orderId,
                request.customerId(),
                request.itemIds(),
                request.totalAmount(),
                OrderStatus.PENDING,
                recordId.getValue(),
                Instant.now()
        );
    }
}
