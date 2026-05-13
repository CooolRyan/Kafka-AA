package com.kafka.active.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class MirrorCompareService {

	private final MirrorReplicationLagService lagService;
	private final AppKafkaProperties kafkaProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;
	private final ObjectMapper objectMapper;

	public MirrorCompareService(
			MirrorReplicationLagService lagService,
			AppKafkaProperties kafkaProperties,
			AppMirrorMetricsProperties mirrorMetricsProperties,
			ObjectMapper objectMapper) {
		this.lagService = lagService;
		this.kafkaProperties = kafkaProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
		this.objectMapper = objectMapper;
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
		List<MirrorPairedRow> paired = buildPairedRows(src, mir);
		return new MirrorCompareDto(lag, srcBoot, mirBoot, src, mir, paired);
	}

	private List<MirrorPairedRow> buildPairedRows(List<TopicTailRecord> src, List<TopicTailRecord> mir) {
		Map<String, TopicTailRecord> srcById = indexFirstByMessageId(src);
		Map<String, TopicTailRecord> mirById = indexFirstByMessageId(mir);
		Set<String> ids = new HashSet<>();
		ids.addAll(srcById.keySet());
		ids.addAll(mirById.keySet());
		List<MirrorPairedRow> rows = new ArrayList<>();
		for (String id : ids) {
			TopicTailRecord s = srcById.get(id);
			TopicTailRecord m = mirById.get(id);
			long sent = sentAtMs(s != null ? s.value() : (m != null ? m.value() : "{}")).orElse(0L);
			Long delta = null;
			if (s != null && m != null) {
				delta = m.timestampMs() - s.timestampMs();
			}
			rows.add(new MirrorPairedRow(id, sent, s, m, delta));
		}
		rows.sort(Comparator.comparingLong(MirrorPairedRow::sentAtMs).reversed());
		return rows;
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
}
