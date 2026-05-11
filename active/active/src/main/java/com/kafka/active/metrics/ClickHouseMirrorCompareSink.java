package com.kafka.active.metrics;

import com.kafka.active.config.AppClickHouseProperties;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;

@Component
@Profile("!test")
public class ClickHouseMirrorCompareSink {

	private static final Logger log = LoggerFactory.getLogger(ClickHouseMirrorCompareSink.class);

	private final AppClickHouseProperties props;
	private final RestClient restClient;

	public ClickHouseMirrorCompareSink(AppClickHouseProperties props, RestClient.Builder restClientBuilder) {
		this.props = props;
		this.restClient = restClientBuilder.build();
	}

	public void write(List<MirrorMessageCompareRow> rows) {
		if (!props.isEnabled() || rows.isEmpty()) {
			return;
		}
		String db = props.getDatabase();
		String query =
				"INSERT INTO "
						+ db
						+ ".mirror_message_compare "
						+ "(message_id, value, source_cluster, mirror_cluster, source_topic, mirror_topic, "
						+ "source_partition, source_offset, mirror_partition, mirror_offset, sent_at_ms, "
						+ "source_kafka_ts_ms, mirror_kafka_ts_ms, source_seen_at_ms, mirror_seen_at_ms, "
						+ "replication_delay_ms, replication_delay_sec, end_to_end_delay_ms, "
						+ "end_to_end_delay_sec, kafka_timestamp_delta_ms) FORMAT TabSeparated";

		StringBuilder body = new StringBuilder();
		for (MirrorMessageCompareRow r : rows) {
			body.append(row(r)).append('\n');
		}

		URI uri =
				UriComponentsBuilder.fromUriString(props.getHttpUrl().replaceAll("/+$", ""))
						.path("/")
						.queryParam("database", db)
						.queryParam("query", query)
						.queryParam("user", props.getUser())
						.queryParam("password", props.getPassword() == null ? "" : props.getPassword())
						.build()
						.toUri();

		try {
			restClient
					.post()
					.uri(uri)
					.contentType(MediaType.TEXT_PLAIN)
					.body(body.toString())
					.retrieve()
					.toBodilessEntity();
		} catch (Exception e) {
			log.warn("clickhouse mirror_message_compare insert failed: {}", e.toString());
		}
	}

	private static String row(MirrorMessageCompareRow r) {
		return String.format(
				Locale.ROOT,
				"%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.3f\t%d\t%.3f\t%d",
				tsvEsc(r.messageId()),
				tsvEsc(r.value()),
				tsvEsc(r.sourceCluster()),
				tsvEsc(r.mirrorCluster()),
				tsvEsc(r.sourceTopic()),
				tsvEsc(r.mirrorTopic()),
				r.sourcePartition(),
				r.sourceOffset(),
				r.mirrorPartition(),
				r.mirrorOffset(),
				r.sentAtMs(),
				r.sourceKafkaTsMs(),
				r.mirrorKafkaTsMs(),
				r.sourceSeenAtMs(),
				r.mirrorSeenAtMs(),
				r.replicationDelayMs(),
				r.replicationDelaySec(),
				r.endToEndDelayMs(),
				r.endToEndDelaySec(),
				r.kafkaTimestampDeltaMs());
	}

	private static String tsvEsc(String v) {
		if (v == null) {
			return "";
		}
		return v.replace("\\", "\\\\")
				.replace("\t", "\\t")
				.replace("\n", "\\n")
				.replace("\r", "\\r");
	}
}
