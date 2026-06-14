package com.boottechnologies.labs;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class SpringBootRedisStreamsApplication {

	static void main(String[] args) {
		SpringApplication.run(SpringBootRedisStreamsApplication.class, args);
	}

}
