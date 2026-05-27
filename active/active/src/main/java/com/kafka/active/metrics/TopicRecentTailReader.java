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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TopicRecentTailReader {

	private static final Logger log = LoggerFactory.getLogger(TopicRecentTailReader.class);

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
		cfg.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
		cfg.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
		cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Math.max(500, limit));

		try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cfg)) {
			List<PartitionInfo> infos = c.partitionsFor(topic);
			if (infos == null || infos.isEmpty()) {
				log.warn("tail read: no partitions topic={} bootstrap={}", topic, bootstrap);
				return List.of();
			}
			List<TopicPartition> tps =
					infos.stream()
							.map(pi -> new TopicPartition(topic, pi.partition()))
							.sorted(Comparator.comparingInt(TopicPartition::partition))
							.toList();
			c.assign(tps);
			Map<TopicPartition, Long> endOffsets = c.endOffsets(tps);
			Map<TopicPartition, Long> beginningOffsets = c.beginningOffsets(tps);
			int n = tps.size();
			int perPart = Math.max(1, (limit + n - 1) / n);

			for (TopicPartition tp : tps) {
				long end = endOffsets.getOrDefault(tp, 0L);
				long begin = beginningOffsets.getOrDefault(tp, 0L);
				if (end <= begin) {
					continue;
				}
				long start = Math.max(begin, end - perPart);
				c.seek(tp, start);
			}
			// seek 반영 + out-of-range 리셋이 poll 전에 일어나도록 1회 워밍업
			c.poll(Duration.ofMillis(500));

			List<TopicTailRecord> buf = new ArrayList<>();
			int emptyPolls = 0;
			final int maxEmptyPolls = 40;
			while (emptyPolls < maxEmptyPolls && buf.size() < limit * 3) {
				ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
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
					long begin = beginningOffsets.getOrDefault(tp, 0L);
					if (end <= begin) {
						continue;
					}
					if (c.position(tp) < end) {
						allCaughtUp = false;
						break;
					}
				}
				if (allCaughtUp && !buf.isEmpty()) {
					break;
				}
				if (allCaughtUp && emptyPolls >= 3) {
					break;
				}
			}
			buf.sort(Comparator.comparingLong(TopicTailRecord::timestampMs).reversed());
			if (buf.size() > limit) {
				return new ArrayList<>(buf.subList(0, limit));
			}
			if (buf.isEmpty()) {
				log.warn(
						"tail read empty topic={} bootstrap={} partitions={} endOffsets={} beginningOffsets={}",
						topic,
						bootstrap,
						tps.size(),
						endOffsets,
						beginningOffsets);
			}
			return buf;
		} catch (Exception e) {
			log.warn("tail read failed topic={} bootstrap={} err={}", topic, bootstrap, e.toString());
			return List.of();
		}
	}
}
