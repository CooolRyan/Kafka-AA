package com.kafka.active.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@Profile("!test")
@ConditionalOnProperty(prefix = "app.kafka", name = "consumer-mode", havingValue = "proxy")
public class ProxyKafkaConsumerConfiguration {

	@Bean
	public ConsumerFactory<String, String> consumerFactoryProxy(AppKafkaProperties props) {
		Map<String, Object> p = new HashMap<>();
		p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProxyBootstrapServers());
		p.put(ConsumerConfig.GROUP_ID_CONFIG, props.getConsumer().getGroupId());
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		return new DefaultKafkaConsumerFactory<>(p);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryProxy(
			ConsumerFactory<String, String> consumerFactoryProxy) {
		var f = new ConcurrentKafkaListenerContainerFactory<String, String>();
		f.setConsumerFactory(consumerFactoryProxy);
		f.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
		return f;
	}
}
