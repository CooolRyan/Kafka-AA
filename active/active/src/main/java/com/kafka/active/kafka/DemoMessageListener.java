package com.kafka.active.kafka;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.failover.IdempotentConsumeHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
@ConditionalOnProperty(
		prefix = "app.kafka",
		name = "consumer-mode",
		havingValue = "dual-listener",
		matchIfMissing = true)
public class DemoMessageListener {

	private final IdempotentConsumeHandler consumeHandler;
	private final AppKafkaProperties kafkaProperties;

	public DemoMessageListener(
			IdempotentConsumeHandler consumeHandler, AppKafkaProperties kafkaProperties) {
		this.consumeHandler = consumeHandler;
		this.kafkaProperties = kafkaProperties;
	}

	@KafkaListener(
			id = "listenPrimary",
			topics = "${app.kafka.consumer.topic}",
			containerFactory = "#{@listenerBinding.primaryContainerFactoryBeanName()}",
			autoStartup = "true")
	public void onPrimary(ConsumerRecord<String, String> record) {
		String cluster = kafkaProperties.getConsumer().getPrimary().trim().toUpperCase();
		consumeHandler.consume("PRIMARY", cluster, record);
	}

	@KafkaListener(
			id = "listenStandby",
			topics = "${app.kafka.consumer.mirror-topic}",
			containerFactory = "#{@listenerBinding.standbyContainerFactoryBeanName()}",
			autoStartup = "false")
	public void onStandby(ConsumerRecord<String, String> record) {
		String primary = kafkaProperties.getConsumer().getPrimary().trim().toUpperCase();
		String standby = "A".equals(primary) ? "B" : "A";
		consumeHandler.consume("STANDBY_MIRROR", standby, record);
	}
}
