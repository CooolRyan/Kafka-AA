package com.kafka.active.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class MirrorMessageCompareService {

	private static final long RETAIN_MS = 10 * 60 * 1000L;

	private final ObjectMapper objectMapper;
	private final Clock clock;
	private final Map<String, Observation> sourceSeen = new HashMap<>();
	private final Map<String, Observation> mirrorSeen = new HashMap<>();
	private final Set<String> emitted = new HashSet<>();

	public MirrorMessageCompareService(ObjectMapper objectMapper) {
		this(objectMapper, Clock.systemUTC());
	}

	MirrorMessageCompareService(ObjectMapper objectMapper, Clock clock) {
		this.objectMapper = objectMapper;
		this.clock = clock;
	}

	public synchronized List<MirrorMessageCompareRow> observeAndCompare(
			MirrorLagSnapshot lag,
			List<TopicTailRecord> sourceRows,
			List<TopicTailRecord> mirrorRows) {
		long now = clock.millis();
		for (TopicTailRecord r : sourceRows) {
			parse(r).ifPresent(m -> sourceSeen.putIfAbsent(m.id(), new Observation(m, now)));
		}
		for (TopicTailRecord r : mirrorRows) {
			parse(r).ifPresent(m -> mirrorSeen.putIfAbsent(m.id(), new Observation(m, now)));
		}

		List<MirrorMessageCompareRow> rows = new ArrayList<>();
		for (var e : sourceSeen.entrySet()) {
			String id = e.getKey();
			Observation source = e.getValue();
			Observation mirror = mirrorSeen.get(id);
			if (mirror == null || emitted.contains(id)) {
				continue;
			}
			long replicationDelayMs = Math.max(0L, mirror.firstSeenAtMs() - source.firstSeenAtMs());
			long endToEndDelayMs = Math.max(0L, mirror.firstSeenAtMs() - source.message().sentAtMs());
			long kafkaTimestampDeltaMs =
					mirror.message().record().timestampMs() - source.message().record().timestampMs();
			rows.add(
					new MirrorMessageCompareRow(
							id,
							source.message().value(),
							lag.sourceCluster(),
							lag.mirrorCluster(),
							lag.sourceTopic(),
							lag.mirrorTopic(),
							source.message().record().partition(),
							source.message().record().offset(),
							mirror.message().record().partition(),
							mirror.message().record().offset(),
							source.message().sentAtMs(),
							source.message().record().timestampMs(),
							mirror.message().record().timestampMs(),
							source.firstSeenAtMs(),
							mirror.firstSeenAtMs(),
							replicationDelayMs,
							replicationDelayMs / 1000.0,
							endToEndDelayMs,
							endToEndDelayMs / 1000.0,
							kafkaTimestampDeltaMs));
			emitted.add(id);
		}
		cleanup(now);
		return rows;
	}

	private java.util.Optional<ParsedMirrorMessage> parse(TopicTailRecord r) {
		if (r.value() == null || !r.value().startsWith("{")) {
			return java.util.Optional.empty();
		}
		try {
			JsonNode n = objectMapper.readTree(r.value());
			String id = text(n, "id");
			if (id == null || id.isBlank()) {
				return java.util.Optional.empty();
			}
			long sentAtMs = n.path("sentAtMs").asLong(0L);
			String value = text(n, "value");
			return java.util.Optional.of(new ParsedMirrorMessage(id, value == null ? r.value() : value, sentAtMs, r));
		} catch (Exception ignored) {
			return java.util.Optional.empty();
		}
	}

	private static String text(JsonNode n, String field) {
		JsonNode v = n.get(field);
		if (v == null || v.isNull()) {
			return null;
		}
		return v.asText();
	}

	private void cleanup(long now) {
		sourceSeen.entrySet().removeIf(e -> now - e.getValue().firstSeenAtMs() > RETAIN_MS);
		mirrorSeen.entrySet().removeIf(e -> now - e.getValue().firstSeenAtMs() > RETAIN_MS);
		if (emitted.size() > 20_000) {
			emitted.clear();
		}
	}

	private record Observation(ParsedMirrorMessage message, long firstSeenAtMs) {}
}
