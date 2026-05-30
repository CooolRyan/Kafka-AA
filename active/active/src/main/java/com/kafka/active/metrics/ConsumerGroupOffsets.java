package com.kafka.active.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.TopicPartition;

public final class ConsumerGroupOffsets {

	private ConsumerGroupOffsets() {}

	public static Map<Integer, Long> committedPerPartition(
			String bootstrap, String groupId, String topic)
			throws ExecutionException, InterruptedException {
		Map<String, Object> cfg = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		try (AdminClient admin = AdminClient.create(cfg)) {
			var result = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
			Map<Integer, Long> out = new HashMap<>();
			for (var e : result.entrySet()) {
				if (e.getKey().topic().equals(topic)) {
					out.put(e.getKey().partition(), e.getValue().offset());
				}
			}
			return out;
		}
	}
}
