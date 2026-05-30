package com.kafka.active.failover;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import com.kafka.active.metrics.MirrorReplicationLagService;
import com.kafka.active.metrics.TopicRecentTailReader;
import com.kafka.active.metrics.TopicTailRecord;
import com.kafka.active.metrics.TopicUnreadScanner;
import com.kafka.active.metrics.TopicUnreadSnapshot;
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
	private static final int MAX_SOURCE_UNREAD_SAMPLE = 300;

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
	 * failover 직전:
	 * 1) 파티션별 committed~end lag
	 * 2) 테스트 run pending id → 미러 tail 매칭
	 * 3) 소스 unread 구간(committed 이후) tail 샘플 id → 미러 tail 매칭
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

		TopicUnreadSnapshot unread =
				TopicUnreadScanner.scan(primaryBoot, sourceTopic, groupId, MAX_SOURCE_UNREAD_SAMPLE);
		var lagSnap = lagService.compute();
		long mirrorLag = lagSnap.lagMessages();

		Map<Integer, Long> mirrorEnd = lagSnap.mirrorHighWatermark() > 0
				? perPartitionEndFromMirror(mirrorBoot, mirrorTopic)
				: Map.of();

		for (var e : unread.committedPerPartition().entrySet()) {
			int p = e.getKey();
			long comm = e.getValue();
			long srcEnd = unread.endPerPartition().getOrDefault(p, 0L);
			long mirEnd = mirrorEnd.getOrDefault(p, 0L);
			long partLag = Math.max(0L, srcEnd - mirEnd);
			clickHouse.insertMirrorPartitionLag(
					runId,
					phase,
					p,
					sourceTopic,
					mirrorTopic,
					comm,
					srcEnd,
					mirEnd,
					partLag);
		}

		int mirrorTailLimit =
				Math.max(300, unread.unreadRecords().size() * 2 + session.pendingMessageIds().size() * 4);
		Map<String, TopicTailRecord> mirrorById =
				indexByMessageId(TopicRecentTailReader.readTail(mirrorBoot, mirrorTopic, mirrorTailLimit));

		PendingCheck pending = checkPending(session.pendingMessageIds(), mirrorById);
		for (var row : pending.rows()) {
			clickHouse.insertMirrorBacklogCheck(
					runId,
					phase,
					"pending_test_id",
					row.id(),
					row.status(),
					unread.committedLagSum(),
					mirrorLag,
					row.sourcePartition(),
					row.sourceOffset(),
					row.mirrorPartition(),
					row.mirrorOffset());
		}

		SourceUnreadCheck sourceUnread =
				checkSourceUnread(unread.unreadRecords(), mirrorById, unread.committedPerPartition());
		for (var row : sourceUnread.rows()) {
			clickHouse.insertMirrorBacklogCheck(
					runId,
					phase,
					"source_unread",
					row.id(),
					row.status(),
					unread.committedLagSum(),
					mirrorLag,
					row.sourcePartition(),
					row.sourceOffset(),
					row.mirrorPartition(),
					row.mirrorOffset());
		}

		List<String> allMissing = new ArrayList<>(pending.missingIds());
		for (String id : sourceUnread.missingIds()) {
			if (allMissing.size() < 30 && !allMissing.contains(id)) {
				allMissing.add(id);
			}
		}

		log.info(
				"mirror backlog run={} phase={} committedLag={} mirrorLag={} pending={}/{} miss={} unreadSample={}/{} miss={}",
				runId,
				phase,
				unread.committedLagSum(),
				mirrorLag,
				pending.mirrored(),
				pending.total(),
				pending.missing(),
				sourceUnread.mirrored(),
				sourceUnread.total(),
				sourceUnread.missing());

		return new MirrorBacklogCheckResult(
				runId,
				phase,
				unread.committedLagSum(),
				mirrorLag,
				pending.total(),
				pending.mirrored(),
				pending.missing(),
				sourceUnread.total(),
				sourceUnread.mirrored(),
				sourceUnread.missing(),
				allMissing,
				sourceUnread.missingIds());
	}

	private Map<Integer, Long> perPartitionEndFromMirror(String bootstrap, String mirrorTopic)
			throws ExecutionException, InterruptedException {
		return com.kafka.active.metrics.TopicEndOffsets.latestPerPartition(bootstrap, mirrorTopic);
	}

	private PendingCheck checkPending(Set<String> pendingIds, Map<String, TopicTailRecord> mirrorById) {
		int mirrored = 0;
		List<String> missing = new ArrayList<>();
		List<CheckRow> rows = new ArrayList<>();
		for (String id : pendingIds) {
			TopicTailRecord mir = mirrorById.get(id);
			boolean ok = mir != null;
			if (ok) {
				mirrored++;
			} else {
				missing.add(id);
			}
			rows.add(
					new CheckRow(
							id,
							ok ? "mirrored" : "missing_on_mirror",
							-1,
							-1L,
							mir == null ? -1 : mir.partition(),
							mir == null ? -1L : mir.offset()));
		}
		return new PendingCheck(pendingIds.size(), mirrored, missing.size(), missing, rows);
	}

	private SourceUnreadCheck checkSourceUnread(
			List<TopicTailRecord> unreadRecords,
			Map<String, TopicTailRecord> mirrorById,
			Map<Integer, Long> committedPerPartition) {
		int mirrored = 0;
		int total = 0;
		List<String> missing = new ArrayList<>();
		List<CheckRow> rows = new ArrayList<>();
		Set<String> seen = new HashSet<>();
		for (TopicTailRecord src : unreadRecords) {
			String id = parseId(src.value()).orElse(null);
			if (id == null || !seen.add(id)) {
				continue;
			}
			total++;
			TopicTailRecord mir = mirrorById.get(id);
			boolean ok = mir != null;
			if (ok) {
				mirrored++;
			} else if (missing.size() < 30) {
				missing.add(id);
			}
			rows.add(
					new CheckRow(
							id,
							ok ? "mirrored" : "missing_on_mirror",
							src.partition(),
							src.offset(),
							mir == null ? -1 : mir.partition(),
							mir == null ? -1L : mir.offset()));
		}
		return new SourceUnreadCheck(total, mirrored, missing.size(), missing, rows);
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

	private record CheckRow(
			String id,
			String status,
			int sourcePartition,
			long sourceOffset,
			int mirrorPartition,
			long mirrorOffset) {}

	private record PendingCheck(
			int total, int mirrored, int missing, List<String> missingIds, List<CheckRow> rows) {}

	private record SourceUnreadCheck(
			int total, int mirrored, int missing, List<String> missingIds, List<CheckRow> rows) {}
}
