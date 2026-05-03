package com.kafka.active.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
@ConditionalOnProperty(prefix = "app.kafka", name = "failover-watchdog-enabled", havingValue = "true", matchIfMissing = true)
public class PrimaryClusterWatchdog {

	private static final Logger log = LoggerFactory.getLogger(PrimaryClusterWatchdog.class);

	private final ClusterReachabilityProbe probe;
	private final ConsumerFailoverCoordinator coordinator;

	public PrimaryClusterWatchdog(
			ClusterReachabilityProbe probe, ConsumerFailoverCoordinator coordinator) {
		this.probe = probe;
		this.coordinator = coordinator;
	}

	@Scheduled(fixedDelayString = "${app.kafka.failover-check-interval-ms:15000}")
	public void tick() {
		if (coordinator.isConsumingOnStandby()) {
			return;
		}
		if (!probe.primaryReachable()) {
			log.error("primary cluster unreachable — failing over to {}", probe.standbyKey());
			coordinator.failoverToStandby();
		}
	}
}
