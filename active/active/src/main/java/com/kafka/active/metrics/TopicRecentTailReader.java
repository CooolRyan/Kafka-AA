package com.kafka.active.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public final class TopicRecentTailReader {

	private TopicRecentTailReader() {}

	/** 토픽 전체 파티션에서 최근 레코드를 합쳐 타임스탬프 역순으로 최대 limit 건 반환 */
	public static List<TopicTailRecord> readTail(String bootstrap, String topic, int limit) {
		if (limit <= 0) {
			return List.of();
		}
		String groupId = "mirror-tail-" + UUID.randomUUID();
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		cfg.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, "mirror-tail-" + groupId.substring(0, 12));
		cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cfg)) {
			List<PartitionInfo> infos = c.partitionsFor(topic);
			if (infos == null || infos.isEmpty()) {
				return List.of();
			}
			List<TopicPartition> tps =
					infos.stream()
							.map(pi -> new TopicPartition(topic, pi.partition()))
							.sorted(Comparator.comparingInt(TopicPartition::partition))
							.toList();
			c.assign(tps);
			Map<TopicPartition, Long> endOffsets = c.endOffsets(tps);
			int n = tps.size();
			int perPart = Math.max(1, (limit + n - 1) / n);
			for (TopicPartition tp : tps) {
				long end = endOffsets.getOrDefault(tp, 0L);
				if (end <= 0) {
					c.seek(tp, 0);
				} else {
					long start = Math.max(0L, end - perPart);
					c.seek(tp, start);
				}
			}
			List<TopicTailRecord> buf = new ArrayList<>();
			int emptyPolls = 0;
			while (emptyPolls < 20) {
				ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(400));
				if (records.isEmpty()) {
					emptyPolls++;
				} else {
					emptyPolls = 0;
					for (ConsumerRecord<String, String> r : records) {
						buf.add(
								new TopicTailRecord(
										r.partition(), r.offset(), r.timestamp(), r.key(), r.value()));
					}
				}
				boolean allCaughtUp = true;
				for (TopicPartition tp : tps) {
					long end = endOffsets.getOrDefault(tp, 0L);
					if (end <= 0) {
						continue;
					}
					if (c.position(tp) < end) {
						allCaughtUp = false;
						break;
					}
				}
				if (allCaughtUp) {
					break;
				}
			}
			buf.sort(Comparator.comparingLong(TopicTailRecord::timestampMs).reversed());
			if (buf.size() > limit) {
				return new ArrayList<>(buf.subList(0, limit));
			}
			return buf;
		}
	}
}
