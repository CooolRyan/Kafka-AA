package com.kafka.active.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class MirrorCompareService {

	/**
	 * UI용 캐시(휘발): Kafka tail에서 잠깐 보였다가 사라지는 문제를 줄이기 위해 최근 페어를 일정 시간 유지한다.
	 * - 키: messageId
	 * - 값: 마지막으로 본(또는 부분적으로 본) source/mirror 레코드 스냅샷
	 */
	private static final long CACHE_TTL_MS = 10 * 60 * 1000L;

	private final MirrorReplicationLagService lagService;
	private final ClickHouseMirrorTailSource clickHouseMirrorTailSource;
	private final AppKafkaProperties kafkaProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;
	private final ObjectMapper objectMapper;
	private final Clock clock;
	private final Map<String, CachedPair> cache = new ConcurrentHashMap<>();

	public MirrorCompareService(
			MirrorReplicationLagService lagService,
			ClickHouseMirrorTailSource clickHouseMirrorTailSource,
			AppKafkaProperties kafkaProperties,
			AppMirrorMetricsProperties mirrorMetricsProperties,
			ObjectMapper objectMapper) {
		this.lagService = lagService;
		this.clickHouseMirrorTailSource = clickHouseMirrorTailSource;
		this.kafkaProperties = kafkaProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
		this.objectMapper = objectMapper;
		this.clock = Clock.systemUTC();
	}

	public MirrorCompareDto compare(int limit) throws ExecutionException, InterruptedException {
		MirrorLagSnapshot lag = lagService.compute();
		String srcKey = mirrorMetricsProperties.getSourceCluster().trim().toUpperCase();
		String mirKey = mirrorMetricsProperties.getMirrorCluster().trim().toUpperCase();
		String srcTopic = mirrorMetricsProperties.getSourceTopic();
		String mirTopic = mirrorMetricsProperties.resolvedMirrorTopic();
		String srcBoot = kafkaProperties.bootstrapFor(srcKey);
		String mirBoot = kafkaProperties.bootstrapFor(mirKey);
		List<TopicTailRecord> src = TopicRecentTailReader.readTail(srcBoot, srcTopic, limit);
		List<TopicTailRecord> mir = TopicRecentTailReader.readTail(mirBoot, mirTopic, limit);
		if (src.isEmpty()
				&& mir.isEmpty()
				&& mirrorMetricsProperties.isCompareFallbackClickhouse()) {
			var fromCh =
					clickHouseMirrorTailSource.loadRecent(
							limit,
							srcTopic,
							mirTopic,
							lag.sourceCluster(),
							lag.mirrorCluster(),
							mirrorMetricsProperties.getCompareClickhouseLookbackMinutes());
			src = fromCh.source();
			mir = fromCh.mirror();
		}
		List<MirrorPairedRow> paired = buildPairedRowsWithCache(src, mir, Math.max(50, limit));
		return new MirrorCompareDto(lag, srcBoot, mirBoot, src, mir, paired);
	}

	private List<MirrorPairedRow> buildPairedRowsWithCache(
			List<TopicTailRecord> src, List<TopicTailRecord> mir, int outLimit) {
		long now = clock.millis();
		evictExpired(now);

		Map<String, TopicTailRecord> srcById = indexFirstByMessageId(src);
		Map<String, TopicTailRecord> mirById = indexFirstByMessageId(mir);
		Set<String> ids = new HashSet<>();
		ids.addAll(srcById.keySet());
		ids.addAll(mirById.keySet());

		// 최신 tail에서 관측된 항목은 캐시에 반영(부분 관측도 유지)
		for (String id : ids) {
			TopicTailRecord s = srcById.get(id);
			TopicTailRecord m = mirById.get(id);
			cache.compute(
					id,
					(k, prev) -> {
						TopicTailRecord srcRec = s != null ? s : (prev == null ? null : prev.source);
						TopicTailRecord mirRec = m != null ? m : (prev == null ? null : prev.mirror);
						long sent =
								sentAtMs(
												srcRec != null
														? srcRec.value()
														: (mirRec != null ? mirRec.value() : "{}"))
										.orElse(prev == null ? 0L : prev.sentAtMs);
						return new CachedPair(srcRec, mirRec, sent, now);
					});
		}

		List<MirrorPairedRow> rows = new ArrayList<>(cache.size());
		for (var e : cache.entrySet()) {
			String id = e.getKey();
			CachedPair cp = e.getValue();
			Long delta = null;
			if (cp.source != null && cp.mirror != null) {
				delta = cp.mirror.timestampMs() - cp.source.timestampMs();
			}
			rows.add(new MirrorPairedRow(id, cp.sentAtMs, cp.source, cp.mirror, delta));
		}
		rows.sort(Comparator.comparingLong(MirrorPairedRow::sentAtMs).reversed());
		if (rows.size() > outLimit) {
			return new ArrayList<>(rows.subList(0, outLimit));
		}
		return rows;
	}

	private void evictExpired(long nowMs) {
		long cutoff = nowMs - CACHE_TTL_MS;
		cache.entrySet().removeIf(e -> e.getValue().updatedAtMs < cutoff);
	}

	private Map<String, TopicTailRecord> indexFirstByMessageId(List<TopicTailRecord> rows) {
		Map<String, TopicTailRecord> map = new HashMap<>();
		for (TopicTailRecord r : rows) {
			parseMessageId(r.value()).ifPresent(id -> map.putIfAbsent(id, r));
		}
		return map;
	}

	private Optional<String> parseMessageId(String json) {
		if (json == null || json.isBlank()) {
			return Optional.empty();
		}
		try {
			JsonNode n = objectMapper.readTree(json);
			if (n.hasNonNull("id") && n.get("id").isTextual()) {
				return Optional.of(n.get("id").asText());
			}
		} catch (Exception ignored) {
			// plain text payload
		}
		return Optional.empty();
	}

	private Optional<Long> sentAtMs(String json) {
		if (json == null || json.isBlank()) {
			return Optional.empty();
		}
		try {
			JsonNode n = objectMapper.readTree(json);
			if (n.has("sentAtMs") && n.get("sentAtMs").isNumber()) {
				return Optional.of(n.get("sentAtMs").asLong());
			}
		} catch (Exception ignored) {
		}
		return Optional.empty();
	}

	private static final class CachedPair {
		private final TopicTailRecord source;
		private final TopicTailRecord mirror;
		private final long sentAtMs;
		private final long updatedAtMs;

		private CachedPair(TopicTailRecord source, TopicTailRecord mirror, long sentAtMs, long updatedAtMs) {
			this.source = source;
			this.mirror = mirror;
			this.sentAtMs = sentAtMs;
			this.updatedAtMs = updatedAtMs;
		}
	}
}
