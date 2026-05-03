package com.kafka.active.kafka;

import com.kafka.active.config.AppKafkaProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component("listenerBinding")
@Profile("!test")
public class ListenerBinding {

	private final AppKafkaProperties props;

	public ListenerBinding(AppKafkaProperties props) {
		this.props = props;
	}

	public String primaryContainerFactoryBeanName() {
		return factoryName(props.getConsumer().getPrimary());
	}

	public String standbyContainerFactoryBeanName() {
		String p = props.getConsumer().getPrimary().trim().toUpperCase();
		String standby = "A".equals(p) ? "B" : "A";
		return factoryName(standby);
	}

	private static String factoryName(String clusterKey) {
		return "kafkaListenerContainerFactory" + clusterKey.trim().toUpperCase();
	}
}
