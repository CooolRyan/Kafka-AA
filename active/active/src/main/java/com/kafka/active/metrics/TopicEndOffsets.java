package com.kafka.active.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;

public final class TopicEndOffsets {

	private TopicEndOffsets() {}

	public static int partitionCount(AdminClient admin, String topic)
			throws ExecutionException, InterruptedException {
		var futures = admin.describeTopics(List.of(topic)).topicNameValues();
		var fd = futures.get(topic);
		if (fd == null) {
			throw new IllegalStateException("topic not found: " + topic);
		}
		TopicDescription d = fd.get();
		return d.partitions().size();
	}

	public static Map<Integer, Long> latestPerPartition(String bootstrap, String topic)
			throws ExecutionException, InterruptedException {
		Map<String, Object> cfg = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		try (AdminClient admin = AdminClient.create(cfg)) {
			int n = partitionCount(admin, topic);
			Map<TopicPartition, OffsetSpec> req = new HashMap<>();
			for (int i = 0; i < n; i++) {
				req.put(new TopicPartition(topic, i), OffsetSpec.latest());
			}
			var infos = admin.listOffsets(req).all().get();
			Map<Integer, Long> out = new HashMap<>();
			for (var e : infos.entrySet()) {
				out.put(e.getKey().partition(), e.getValue().offset());
			}
			return out;
		}
	}
}
