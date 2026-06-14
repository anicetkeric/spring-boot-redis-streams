package com.boottechnologies.labs.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank(message = "customerId is required")
        String customerId,

        @NotEmpty(message = "itemIds must not be empty")
        List<String> itemIds,

        @NotNull(message = "totalAmount is required")
        @DecimalMin(value = "0.01", message = "totalAmount must be greater than 0")
        BigDecimal totalAmount
) {}
