package com.boottechnologies.labs;

import com.boottechnologies.labs.dto.CreateOrderRequest;
import com.boottechnologies.labs.dto.OrderResponse;
import com.boottechnologies.labs.domain.OrderStatus;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderStreamIntegrationTest {

    @Container
    @ServiceConnection
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine"))
            .withExposedPorts(6379);

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    StringRedisTemplate redisTemplate;

    @Test
    void shouldPublishOrderEventToStream() {
        CreateOrderRequest request = new CreateOrderRequest(
                "customer-42",
                List.of("item-1", "item-2"),
                new BigDecimal("149.99")
        );

        ResponseEntity<OrderResponse> response = restTemplate.postForEntity(
                "/api/v1/orders", request, OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().orderId()).isNotBlank();
        assertThat(response.getBody().status()).isEqualTo(OrderStatus.PENDING);
        assertThat(response.getBody().streamRecordId()).isNotBlank();
    }

    @Test
    void shouldHaveMessagesInStream() {
        CreateOrderRequest request = new CreateOrderRequest(
                "customer-99",
                List.of("item-A"),
                new BigDecimal("29.99")
        );

        restTemplate.postForEntity("/api/v1/orders", request, OrderResponse.class);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Long streamLen = redisTemplate.opsForStream().size("orders:events");
            assertThat(streamLen).isGreaterThan(0);
        });
    }

    @Test
    void shouldRejectInvalidOrderRequest() {
        CreateOrderRequest invalid = new CreateOrderRequest(
                "",
                List.of(),
                BigDecimal.ZERO
        );

        ResponseEntity<String> response = restTemplate.postForEntity(
                "/api/v1/orders", invalid, String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}
