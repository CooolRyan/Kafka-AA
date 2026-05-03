package com.kafka.active.web;

import com.kafka.active.kafka.ClusterReachabilityProbe;
import com.kafka.active.kafka.ConsumerFailoverCoordinator;
import com.kafka.active.kafka.DualClusterTrafficProducer;
import java.util.Map;
import org.springframework.http.ResponseEntity;
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

	public DemoKafkaController(
			DualClusterTrafficProducer producer,
			ConsumerFailoverCoordinator failoverCoordinator,
			ClusterReachabilityProbe probe) {
		this.producer = producer;
		this.failoverCoordinator = failoverCoordinator;
		this.probe = probe;
	}

	@PostMapping("/produce")
	public ResponseEntity<Map<String, Object>> produce(
			@RequestParam(defaultValue = "20") int count,
			@RequestParam(defaultValue = "demo") String prefix) {
		producer.sendBatch(count, prefix);
		return ResponseEntity.ok(Map.of("requested", count, "mode", "round-robin-50-50-A-B"));
	}

	@PostMapping("/failover/{target}")
	public ResponseEntity<Map<String, Object>> failover(@PathVariable String target) {
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
