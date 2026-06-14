package com.boottechnologies.labs.config;

import com.boottechnologies.labs.consumer.InventoryConsumer;
import com.boottechnologies.labs.consumer.NotificationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;

import java.time.Duration;
import java.util.concurrent.Executors;

@Configuration
public class StreamConfig {

    private static final Logger log = LoggerFactory.getLogger(StreamConfig.class);

    private final StreamProperties props;
    private final StringRedisTemplate redisTemplate;

    public StreamConfig(StreamProperties props, StringRedisTemplate redisTemplate) {
        this.props = props;
        this.redisTemplate = redisTemplate;
    }

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> listenerContainer(
            RedisConnectionFactory connectionFactory) {

        var options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .<String, MapRecord<String, String, String>>builder()
                .pollTimeout(Duration.ofMillis(100))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        return StreamMessageListenerContainer.create(connectionFactory, options);
    }

    @Bean
    public ApplicationRunner startStreamListeners(
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> container,
            InventoryConsumer inventoryConsumer,
            NotificationConsumer notificationConsumer) {

        return args -> {
            ensureConsumerGroupExists(props.streamKey(), props.inventoryGroup());
            ensureConsumerGroupExists(props.streamKey(), props.notificationGroup());

            Subscription inventorySub = container.receive(
                    Consumer.from(props.inventoryGroup(), props.inventoryConsumer()),
                    StreamOffset.create(props.streamKey(), ReadOffset.lastConsumed()),
                    inventoryConsumer
            );

            Subscription notificationSub = container.receive(
                    Consumer.from(props.notificationGroup(), props.notificationConsumer()),
                    StreamOffset.create(props.streamKey(), ReadOffset.lastConsumed()),
                    notificationConsumer
            );

            container.start();
            log.info("Stream listeners started. inventory={} notification={}",
                    inventorySub.isActive(), notificationSub.isActive());
        };
    }

    private void ensureConsumerGroupExists(String streamKey, String groupName) {
        try {
            redisTemplate.opsForStream().createGroup(streamKey, ReadOffset.from("0"), groupName);
            log.info("Consumer group created: stream={} group={}", streamKey, groupName);
        } catch (Exception e) {
            Throwable cause = e.getCause();

            if (cause != null
                    && cause.getMessage() != null
                    && cause.getMessage().contains("BUSYGROUP")) {
                log.debug("Consumer group already exists: stream={} group={}", streamKey, groupName);
            } else {
                throw e;
            }
        }
    }
}
