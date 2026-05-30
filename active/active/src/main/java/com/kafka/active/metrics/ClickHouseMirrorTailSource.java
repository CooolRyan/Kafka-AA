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

	/**
	 * INSERT 시각(ts) 기준 최신 N건을 소스/미러 각각 조회한다. (시간 구간 필터 없음)
	 *
	 * @param limitPerRole 소스·미러 각각 가져올 최대 row 수
	 */
	public TailFromClickHouse loadRecent(
			int limitPerRole,
			String sourceTopic,
			String mirrorTopic,
			String sourceCluster,
			String mirrorCluster) {
		if (!props.isEnabled()) {
			return new TailFromClickHouse(List.of(), List.of());
		}
		int lim = Math.max(1, Math.min(500, limitPerRole));
		try {
			List<TopicTailRecord> src =
					queryLatestByRole("source", sourceTopic, sourceCluster, lim);
			List<TopicTailRecord> mir =
					queryLatestByRole("mirror", mirrorTopic, mirrorCluster, lim);
			if (!src.isEmpty() || !mir.isEmpty()) {
				log.info(
						"compare UI: loaded from ClickHouse src={} mir={} limitPerRole={}",
						src.size(),
						mir.size(),
						lim);
			}
			return new TailFromClickHouse(src, mir);
		} catch (Exception e) {
			log.warn("clickhouse tail load failed: {}", e.toString());
			return new TailFromClickHouse(List.of(), List.of());
		}
	}

	private List<TopicTailRecord> queryLatestByRole(
			String role, String topic, String cluster, int limit) throws Exception {
		String db = props.getDatabase();
		String sql =
				"SELECT role, cluster, topic, partition, offset, kafka_ts_ms, value "
						+ "FROM "
						+ db
						+ ".mirror_message_tail "
						+ "WHERE role = '"
						+ escSql(role)
						+ "' AND topic = '"
						+ escSql(topic)
						+ "' AND cluster = '"
						+ escSql(cluster)
						+ "' ORDER BY ts DESC LIMIT "
						+ limit
						+ " FORMAT JSON";

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
			return List.of();
		}
		JsonNode root = objectMapper.readTree(body);
		JsonNode data = root.get("data");
		if (data == null || !data.isArray()) {
			return List.of();
		}
		List<TopicTailRecord> out = new ArrayList<>();
		for (JsonNode row : data) {
			int partition = row.get("partition").asInt();
			long offset = row.get("offset").asLong();
			long kafkaTs = row.get("kafka_ts_ms").asLong();
			String value = text(row, "value");
			out.add(new TopicTailRecord(partition, offset, kafkaTs, null, value));
		}
		return out;
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
