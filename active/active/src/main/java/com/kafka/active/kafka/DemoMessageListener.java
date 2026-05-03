package com.kafka.active.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class DemoMessageListener {

	private static final Logger log = LoggerFactory.getLogger(DemoMessageListener.class);

	@KafkaListener(
			id = "listenPrimary",
			topics = "${app.kafka.consumer.topic}",
			containerFactory = "#{@listenerBinding.primaryContainerFactoryBeanName()}",
			autoStartup = "true")
	public void onPrimary(String value) {
		log.info("[consume][PRIMARY] {}", value);
	}

	@KafkaListener(
			id = "listenStandby",
			topics = "${app.kafka.consumer.topic}",
			containerFactory = "#{@listenerBinding.standbyContainerFactoryBeanName()}",
			autoStartup = "false")
	public void onStandby(String value) {
		log.info("[consume][STANDBY] {}", value);
	}
}
