package com.kafka.active;

import com.kafka.active.config.AppClickHouseProperties;
import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({
	AppKafkaProperties.class,
	AppClickHouseProperties.class,
	AppMirrorMetricsProperties.class
})
public class ActiveApplication {

	public static void main(String[] args) {
		SpringApplication.run(ActiveApplication.class, args);
	}

}
