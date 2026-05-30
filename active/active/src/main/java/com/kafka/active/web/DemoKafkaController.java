package com.kafka.active.web;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.kafka.ClusterReachabilityProbe;
import com.kafka.active.kafka.ConsumerFailoverCoordinator;
import com.kafka.active.kafka.DualClusterTrafficProducer;
import java.util.List;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Profile("!test")
public class DemoKafkaController {

	private final DualClusterTrafficProducer producer;
	private final ConsumerFailoverCoordinator failoverCoordinator;
	private final ClusterReachabilityProbe probe;
	private final AppKafkaProperties kafkaProperties;

	public DemoKafkaController(
			DualClusterTrafficProducer producer,
			ConsumerFailoverCoordinator failoverCoordinator,
			ClusterReachabilityProbe probe,
			AppKafkaProperties kafkaProperties) {
		this.producer = producer;
		this.failoverCoordinator = failoverCoordinator;
		this.probe = probe;
		this.kafkaProperties = kafkaProperties;
	}

	@PostMapping("/produce")
	public ResponseEntity<Map<String, Object>> produce(
			@RequestParam(defaultValue = "20") int count,
			@RequestParam(defaultValue = "demo") String prefix) {
		List<String> ids = producer.sendBatch(count, prefix);
		return ResponseEntity.ok(
				Map.of(
						"requested", count,
						"mode", "round-robin-50-50-A-B",
						"payload", "json-id-sentAtMs",
						"sampleIds", ids.stream().limit(10).toList()));
	}

	@GetMapping("/failover/status")
	public ResponseEntity<Map<String, Object>> failoverStatus() {
		String primary = kafkaProperties.getConsumer().getPrimary().trim().toUpperCase();
		String standby = probe.standbyKey();
		return ResponseEntity.ok(
				Map.of(
						"consumingOnStandby", failoverCoordinator.isConsumingOnStandby(),
						"activeConsumer", failoverCoordinator.isConsumingOnStandby() ? standby : primary,
						"consumerTopic", kafkaProperties.getConsumer().getTopic(),
						"consumerGroupId", kafkaProperties.getConsumer().getGroupId(),
						"primaryCluster", primary,
						"standbyCluster", standby,
						"primaryReachable", probe.primaryReachable(),
						"clusterAReachable", probe.canReachCluster("A"),
						"clusterBReachable", probe.canReachCluster("B")));
	}

	@GetMapping("/failover/mode")
	public ResponseEntity<Map<String, Object>> failoverMode() {
		return ResponseEntity.ok(
				Map.of(
						"consumerMode",
						kafkaProperties.isProxyConsumerMode() ? "proxy" : "dual-listener",
						"proxyBootstrap",
						kafkaProperties.isProxyConsumerMode()
								? kafkaProperties.getProxyBootstrapServers()
								: "",
						"hint",
						kafkaProperties.isProxyConsumerMode()
								? "Use HAProxy active/backup; stop cluster A to fail over"
								: "Use POST /api/failover/standby or watchdog"));
	}

	@PostMapping("/failover/{target}")
	public ResponseEntity<Map<String, Object>> failover(@PathVariable String target) {
		if (kafkaProperties.isProxyConsumerMode()) {
			return ResponseEntity.badRequest()
					.body(
							Map.of(
									"error",
									"consumer-mode=proxy: use HAProxy failover (stop cluster A), not app listener switch"));
		}
		return switch (target.toLowerCase()) {
			case "standby" -> {
				failoverCoordinator.failoverToStandby();
				yield ResponseEntity.ok(Map.of("state", "standby"));
			}
			case "primary" -> {
				failoverCoordinator.failbackToPrimary();
				yield ResponseEntity.ok(Map.of("state", "primary"));
			}
			default -> ResponseEntity.badRequest().body(Map.of("error", "use standby or primary"));
		};
	}

	@PostMapping("/probe")
	public ResponseEntity<Map<String, Object>> probe(
			@RequestParam String cluster) {
		boolean ok = probe.canReachCluster(cluster.trim().toUpperCase());
		return ResponseEntity.ok(Map.of("cluster", cluster, "reachable", ok));
	}
}
