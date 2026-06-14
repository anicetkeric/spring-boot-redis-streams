package com.boottechnologies.labs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.streams.orders")
public record StreamProperties(
        String streamKey,
        String dlqKey,
        long maxLen,
        String inventoryGroup,
        String notificationGroup,
        String inventoryConsumer,
        String notificationConsumer,
        long claimMinIdleMs
) {}
