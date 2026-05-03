package com.kafka.active.kafka;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class ConsumerFailoverCoordinator {

	private static final Logger log = LoggerFactory.getLogger(ConsumerFailoverCoordinator.class);

	private static final String PRIMARY_ID = "listenPrimary";
	private static final String STANDBY_ID = "listenStandby";

	private final KafkaListenerEndpointRegistry registry;
	private volatile boolean consumingOnStandby;

	public ConsumerFailoverCoordinator(KafkaListenerEndpointRegistry registry) {
		this.registry = registry;
	}

	public boolean isConsumingOnStandby() {
		return consumingOnStandby;
	}

	public synchronized void failoverToStandby() {
		MessageListenerContainer primary = registry.getListenerContainer(PRIMARY_ID);
		MessageListenerContainer standby = registry.getListenerContainer(STANDBY_ID);
		Objects.requireNonNull(primary, PRIMARY_ID);
		Objects.requireNonNull(standby, STANDBY_ID);
		if (consumingOnStandby) {
			log.info("failover skipped: already on standby");
			return;
		}
		log.warn("stopping primary consumer, starting standby");
		primary.stop();
		standby.start();
		consumingOnStandby = true;
	}

	public synchronized void failbackToPrimary() {
		MessageListenerContainer primary = registry.getListenerContainer(PRIMARY_ID);
		MessageListenerContainer standby = registry.getListenerContainer(STANDBY_ID);
		Objects.requireNonNull(primary, PRIMARY_ID);
		Objects.requireNonNull(standby, STANDBY_ID);
		if (!consumingOnStandby) {
			log.info("failback skipped: already on primary");
			return;
		}
		log.warn("stopping standby consumer, starting primary");
		standby.stop();
		primary.start();
		consumingOnStandby = false;
	}
}
