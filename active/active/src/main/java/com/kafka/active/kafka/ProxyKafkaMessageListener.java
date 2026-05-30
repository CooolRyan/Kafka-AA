package com.kafka.active.kafka;

import com.kafka.active.failover.IdempotentConsumeHandler;
import com.kafka.active.config.AppKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** HAProxy 단일 bootstrap — 인프라 failover 시 재연결만으로 backend 전환 */
@Component
@Profile("!test")
@ConditionalOnProperty(prefix = "app.kafka", name = "consumer-mode", havingValue = "proxy")
public class ProxyKafkaMessageListener {

	private static final Logger log = LoggerFactory.getLogger(ProxyKafkaMessageListener.class);

	private final IdempotentConsumeHandler consumeHandler;
	private final AppKafkaProperties kafkaProperties;

	public ProxyKafkaMessageListener(
			IdempotentConsumeHandler consumeHandler, AppKafkaProperties kafkaProperties) {
		this.consumeHandler = consumeHandler;
		this.kafkaProperties = kafkaProperties;
	}

	/** HAProxy L4 failover 후 B 에 붙을 때 MM2 미러 토픽 consume */
	@KafkaListener(
			id = "listenProxy",
			topics = "${app.kafka.consumer.mirror-topic}",
			containerFactory = "kafkaListenerContainerFactoryProxy",
			autoStartup = "true")
	public void onMessage(ConsumerRecord<String, String> record) {
		String mirrorKey = kafkaProperties.getConsumer().getPrimary().equalsIgnoreCase("A") ? "B" : "A";
		consumeHandler.consume("PROXY_MIRROR", mirrorKey, record);
	}
}
