package com.kafka.active.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@EnableKafka
@Profile("!test")
public class DualClusterKafkaConfiguration {

	private static Map<String, Object> producerProps(String bootstrap) {
		Map<String, Object> p = new HashMap<>();
		p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		p.put(ProducerConfig.ACKS_CONFIG, "all");
		return p;
	}

	private static Map<String, Object> consumerProps(String bootstrap, String groupId) {
		Map<String, Object> p = new HashMap<>();
		p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		return p;
	}

	@Bean
	public KafkaAdmin kafkaAdminA(AppKafkaProperties props) {
		return kafkaAdmin(props.bootstrapFor("A"));
	}

	@Bean
	public KafkaAdmin kafkaAdminB(AppKafkaProperties props) {
		return kafkaAdmin(props.bootstrapFor("B"));
	}

	private static KafkaAdmin kafkaAdmin(String bootstrap) {
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		return new KafkaAdmin(cfg);
	}

	@Bean
	public ProducerFactory<String, String> producerFactoryA(AppKafkaProperties props) {
		return new DefaultKafkaProducerFactory<>(producerProps(props.bootstrapFor("A")));
	}

	@Bean
	public ProducerFactory<String, String> producerFactoryB(AppKafkaProperties props) {
		return new DefaultKafkaProducerFactory<>(producerProps(props.bootstrapFor("B")));
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplateA(
			@Qualifier("producerFactoryA") ProducerFactory<String, String> pf) {
		return new KafkaTemplate<>(pf);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplateB(
			@Qualifier("producerFactoryB") ProducerFactory<String, String> pf) {
		return new KafkaTemplate<>(pf);
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactoryA(AppKafkaProperties props) {
		var c = props.getConsumer();
		return new DefaultKafkaConsumerFactory<>(
				consumerProps(props.bootstrapFor("A"), c.getGroupId()));
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactoryB(AppKafkaProperties props) {
		var c = props.getConsumer();
		return new DefaultKafkaConsumerFactory<>(
				consumerProps(props.bootstrapFor("B"), c.getGroupId()));
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryA(
			@Qualifier("consumerFactoryA") ConsumerFactory<String, String> cf) {
		return listenerFactory(cf);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryB(
			@Qualifier("consumerFactoryB") ConsumerFactory<String, String> cf) {
		return listenerFactory(cf);
	}

	private static ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory(
			ConsumerFactory<String, String> cf) {
		var f = new ConcurrentKafkaListenerContainerFactory<String, String>();
		f.setConsumerFactory(cf);
		f.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
		return f;
	}
}
