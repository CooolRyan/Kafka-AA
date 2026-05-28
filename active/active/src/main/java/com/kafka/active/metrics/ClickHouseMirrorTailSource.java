package com.kafka.active.metrics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppClickHouseProperties;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

/** Kafka tail 이 비었을 때 ClickHouse mirror_message_tail 스냅샷으로 UI를 채운다. */
@Component
@Profile("!test")
public class ClickHouseMirrorTailSource {

	private static final Logger log = LoggerFactory.getLogger(ClickHouseMirrorTailSource.class);

	private final AppClickHouseProperties props;
	private final RestClient restClient;
	private final ObjectMapper objectMapper;

	public ClickHouseMirrorTailSource(
			AppClickHouseProperties props, RestClient.Builder restClientBuilder, ObjectMapper objectMapper) {
		this.props = props;
		this.restClient = restClientBuilder.build();
		this.objectMapper = objectMapper;
	}

	public TailFromClickHouse loadRecent(
			int limit,
			String sourceTopic,
			String mirrorTopic,
			String sourceCluster,
			String mirrorCluster,
			int lookbackMinutes) {
		if (!props.isEnabled()) {
			return new TailFromClickHouse(List.of(), List.of());
		}
		int lim = Math.max(10, Math.min(500, limit * 4));
		int mins = Math.max(1, lookbackMinutes);
		String db = props.getDatabase();
		String sql =
				"SELECT role, cluster, topic, partition, offset, kafka_ts_ms, value "
						+ "FROM "
						+ db
						+ ".mirror_message_tail "
						+ "WHERE ts > now() - INTERVAL "
						+ mins
						+ " MINUTE "
						+ "AND ((role = 'source' AND topic = '"
						+ escSql(sourceTopic)
						+ "' AND cluster = '"
						+ escSql(sourceCluster)
						+ "') OR (role = 'mirror' AND topic = '"
						+ escSql(mirrorTopic)
						+ "' AND cluster = '"
						+ escSql(mirrorCluster)
						+ "')) "
						+ "ORDER BY ts DESC "
						+ "LIMIT "
						+ lim
						+ " FORMAT JSON";

		try {
			URI uri =
					UriComponentsBuilder.fromUriString(props.getHttpUrl().replaceAll("/+$", ""))
							.path("/")
							.queryParam("database", db)
							.queryParam("query", sql)
							.queryParam("user", props.getUser())
							.queryParam("password", props.getPassword() == null ? "" : props.getPassword())
							.build()
							.toUri();
			String body = restClient.get().uri(uri).retrieve().body(String.class);
			if (body == null || body.isBlank()) {
				return new TailFromClickHouse(List.of(), List.of());
			}
			JsonNode root = objectMapper.readTree(body);
			JsonNode data = root.get("data");
			if (data == null || !data.isArray()) {
				return new TailFromClickHouse(List.of(), List.of());
			}
			List<TopicTailRecord> src = new ArrayList<>();
			List<TopicTailRecord> mir = new ArrayList<>();
			for (JsonNode row : data) {
				String role = text(row, "role");
				int partition = row.get("partition").asInt();
				long offset = row.get("offset").asLong();
				long kafkaTs = row.get("kafka_ts_ms").asLong();
				String value = text(row, "value");
				TopicTailRecord rec = new TopicTailRecord(partition, offset, kafkaTs, null, value);
				if ("source".equalsIgnoreCase(role)) {
					src.add(rec);
				} else if ("mirror".equalsIgnoreCase(role)) {
					mir.add(rec);
				}
			}
			if (!src.isEmpty() || !mir.isEmpty()) {
				log.info(
						"compare UI: loaded from ClickHouse src={} mir={} lookbackMin={}",
						src.size(),
						mir.size(),
						mins);
			}
			return new TailFromClickHouse(src, mir);
		} catch (Exception e) {
			log.warn("clickhouse tail load failed: {}", e.toString());
			return new TailFromClickHouse(List.of(), List.of());
		}
	}

	private static String text(JsonNode row, String field) {
		JsonNode n = row.get(field);
		return n == null || n.isNull() ? "" : n.asText();
	}

	private static String escSql(String v) {
		return v == null ? "" : v.replace("'", "''");
	}

	public record TailFromClickHouse(List<TopicTailRecord> source, List<TopicTailRecord> mirror) {}
}
