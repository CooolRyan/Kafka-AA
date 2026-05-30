package com.kafka.active.failover;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import com.kafka.active.metrics.ConsumerGroupOffsets;
import com.kafka.active.metrics.MirrorReplicationLagService;
import com.kafka.active.metrics.TopicEndOffsets;
import com.kafka.active.metrics.TopicRecentTailReader;
import com.kafka.active.metrics.TopicTailRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class FailoverMirrorBacklogVerifier {

	private static final Logger log = LoggerFactory.getLogger(FailoverMirrorBacklogVerifier.class);

	private final AppKafkaProperties kafkaProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;
	private final MirrorReplicationLagService lagService;
	private final ClickHouseFailoverSink clickHouse;
	private final ObjectMapper objectMapper;

	public FailoverMirrorBacklogVerifier(
			AppKafkaProperties kafkaProperties,
			AppMirrorMetricsProperties mirrorMetricsProperties,
			MirrorReplicationLagService lagService,
			ClickHouseFailoverSink clickHouse,
			ObjectMapper objectMapper) {
		this.kafkaProperties = kafkaProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
		this.lagService = lagService;
		this.clickHouse = clickHouse;
		this.objectMapper = objectMapper;
	}

	/**
	 * failover 직전: (1) primary committed~HWM lag (2) 테스트 run 에서 아직 consume 안 된 id 가 미러 토픽 tail 에 있는지.
	 */
	public MirrorBacklogCheckResult verifyBeforeFailover(FailoverActiveSession session, String phase)
			throws ExecutionException, InterruptedException {
		String runId = session.runId();
		String primaryKey = kafkaProperties.getConsumer().getPrimary().trim().toUpperCase();
		String mirrorKey = mirrorMetricsProperties.getMirrorCluster().trim().toUpperCase();
		String sourceTopic = kafkaProperties.getConsumer().getTopic();
		String mirrorTopic = kafkaProperties.getConsumer().resolvedMirrorTopic(primaryKey);
		String primaryBoot = kafkaProperties.bootstrapFor(primaryKey);
		String mirrorBoot = kafkaProperties.bootstrapFor(mirrorKey);
		String groupId = kafkaProperties.getConsumer().getGroupId();

		long committedLagSum = committedLagSum(primaryBoot, sourceTopic, groupId);
		var lagSnap = lagService.compute();
		long mirrorLag = lagSnap.lagMessages();

		Set<String> pendingIds = session.pendingMessageIds();
		Map<String, TopicTailRecord> mirrorById = indexByMessageId(
				TopicRecentTailReader.readTail(mirrorBoot, mirrorTopic, Math.max(200, pendingIds.size() * 4)));

		int mirrored = 0;
		List<String> missing = new ArrayList<>();
		for (String id : pendingIds) {
			boolean onMirror = mirrorById.containsKey(id);
			if (onMirror) {
				mirrored++;
			} else {
				missing.add(id);
			}
			TopicTailRecord mir = mirrorById.get(id);
			clickHouse.insertMirrorBacklogCheck(
					runId,
					phase,
					id,
					onMirror ? "mirrored" : "missing_on_mirror",
					committedLagSum,
					mirrorLag,
					mir == null ? -1 : mir.partition(),
					mir == null ? -1L : mir.offset());
		}

		List<String> missingSample = missing.size() > 20 ? missing.subList(0, 20) : missing;
		log.info(
				"mirror backlog check run={} phase={} pending={} mirrored={} missing={} committedLagSum={} mirrorLag={}",
				runId,
				phase,
				pendingIds.size(),
				mirrored,
				missing.size(),
				committedLagSum,
				mirrorLag);

		return new MirrorBacklogCheckResult(
				runId,
				phase,
				committedLagSum,
				mirrorLag,
				pendingIds.size(),
				mirrored,
				missing.size(),
				missingSample);
	}

	private long committedLagSum(String bootstrap, String topic, String groupId)
			throws ExecutionException, InterruptedException {
		Map<Integer, Long> end = TopicEndOffsets.latestPerPartition(bootstrap, topic);
		Map<Integer, Long> committed =
				ConsumerGroupOffsets.committedPerPartition(bootstrap, groupId, topic);
		long lag = 0;
		for (var e : end.entrySet()) {
			int p = e.getKey();
			long endOff = e.getValue();
			long comm = committed.getOrDefault(p, 0L);
			lag += Math.max(0L, endOff - comm);
		}
		return lag;
	}

	private Map<String, TopicTailRecord> indexByMessageId(List<TopicTailRecord> rows) {
		Map<String, TopicTailRecord> map = new HashMap<>();
		for (TopicTailRecord r : rows) {
			parseId(r.value()).ifPresent(id -> map.putIfAbsent(id, r));
		}
		return map;
	}

	private java.util.Optional<String> parseId(String json) {
		if (json == null || json.isBlank()) {
			return java.util.Optional.empty();
		}
		try {
			JsonNode n = objectMapper.readTree(json);
			if (n.hasNonNull("id")) {
				return java.util.Optional.of(n.get("id").asText());
			}
		} catch (Exception ignored) {
		}
		return java.util.Optional.empty();
	}
}
