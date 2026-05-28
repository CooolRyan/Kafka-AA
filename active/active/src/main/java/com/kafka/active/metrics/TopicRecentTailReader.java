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
		try (KafkaConsumer<String, String> meta = new KafkaConsumer<>(consumerConfig(bootstrap, "meta"))) {
			List<PartitionInfo> infos = meta.partitionsFor(topic);
			if (infos == null || infos.isEmpty()) {
				log.warn("tail read: no partitions topic={} bootstrap={}", topic, bootstrap);
				return List.of();
			}
			List<TopicPartition> allTps =
					infos.stream()
							.map(pi -> new TopicPartition(topic, pi.partition()))
							.sorted(Comparator.comparingInt(TopicPartition::partition))
							.toList();
			meta.assign(allTps);
			Map<TopicPartition, Long> endOffsets = meta.endOffsets(allTps);
			Map<TopicPartition, Long> beginningOffsets = meta.beginningOffsets(allTps);

			List<TopicPartition> readable = new ArrayList<>();
			for (TopicPartition tp : allTps) {
				if (endOffsets.getOrDefault(tp, 0L) > beginningOffsets.getOrDefault(tp, 0L)) {
					readable.add(tp);
				}
			}
			if (readable.isEmpty()) {
				log.warn(
						"tail read: no readable partitions topic={} bootstrap={} end={} begin={}",
						topic,
						bootstrap,
						endOffsets,
						beginningOffsets);
				return List.of();
			}

			int perPart = Math.max(1, (limit + readable.size() - 1) / readable.size());
			List<TopicTailRecord> buf = new ArrayList<>();
			for (TopicPartition tp : readable) {
				long end = endOffsets.get(tp);
				long begin = beginningOffsets.get(tp);
				buf.addAll(readPartitionTail(bootstrap, tp, begin, end, perPart));
			}
			buf.sort(Comparator.comparingLong(TopicTailRecord::timestampMs).reversed());
			if (buf.size() > limit) {
				return new ArrayList<>(buf.subList(0, limit));
			}
			if (buf.isEmpty()) {
				log.warn(
						"tail read empty topic={} bootstrap={} readable={} end={} begin={}",
						topic,
						bootstrap,
						readable,
						endOffsets,
						beginningOffsets);
			}
			return buf;
		} catch (Exception e) {
			log.warn("tail read failed topic={} bootstrap={} err={}", topic, bootstrap, e.toString());
			return List.of();
		}
	}

	private static List<TopicTailRecord> readPartitionTail(
			String bootstrap, TopicPartition tp, long begin, long end, int maxRecords) {
		String runId = UUID.randomUUID().toString();
		long start = Math.max(begin, end - maxRecords);
		List<TopicTailRecord> out = new ArrayList<>();
		try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerConfig(bootstrap, runId))) {
			c.assign(List.of(tp));
			c.seek(tp, start);
			int empty = 0;
			while (empty < 30 && out.size() < maxRecords * 2 && c.position(tp) < end) {
				ConsumerRecords<String, String> records = c.poll(Duration.ofMillis(500));
				if (records.isEmpty()) {
					empty++;
				} else {
					empty = 0;
					for (ConsumerRecord<String, String> r : records) {
						out.add(
								new TopicTailRecord(
										r.partition(), r.offset(), r.timestamp(), r.key(), r.value()));
					}
				}
			}
		} catch (Exception e) {
			log.warn(
					"tail read partition failed tp={} bootstrap={} begin={} end={} err={}",
					tp,
					bootstrap,
					begin,
					end,
					e.toString());
		}
		return out;
	}

	private static Map<String, Object> consumerConfig(String bootstrap, String runId) {
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "mirror-tail-" + runId);
		cfg.put(ConsumerConfig.CLIENT_ID_CONFIG, "mirror-tail-" + runId);
		cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		cfg.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
		cfg.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000);
		cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		return cfg;
	}
}
