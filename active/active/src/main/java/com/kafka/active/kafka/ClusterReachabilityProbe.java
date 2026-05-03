package com.kafka.active.kafka;

import com.kafka.active.config.AppKafkaProperties;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class ClusterReachabilityProbe {

	private final AppKafkaProperties props;

	public ClusterReachabilityProbe(AppKafkaProperties props) {
		this.props = props;
	}

	public boolean canReachCluster(String clusterKey) {
		Map<String, Object> cfg = Map.of(
				AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				props.bootstrapFor(clusterKey),
				AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,
				3_000);
		try (AdminClient admin = AdminClient.create(cfg)) {
			admin.describeCluster().clusterId().get();
			return true;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		} catch (ExecutionException | RuntimeException e) {
			return false;
		}
	}

	public boolean primaryReachable() {
		return canReachCluster(props.getConsumer().getPrimary());
	}

	public String standbyKey() {
		String p = props.getConsumer().getPrimary().trim().toUpperCase();
		return "A".equals(p) ? "B" : "A";
	}
}
