package com.kafka.active.metrics;

import com.kafka.active.config.AppClickHouseProperties;
import java.net.URI;
import java.util.ArrayList;
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
public class ClickHouseMirrorTailSink {

	private static final Logger log = LoggerFactory.getLogger(ClickHouseMirrorTailSink.class);

	private final AppClickHouseProperties props;
	private final RestClient restClient;

	public ClickHouseMirrorTailSink(AppClickHouseProperties props, RestClient.Builder restClientBuilder) {
		this.props = props;
		this.restClient = restClientBuilder.build();
	}

	public void write(
			MirrorLagSnapshot lag,
			List<TopicTailRecord> sourceRows,
			List<TopicTailRecord> mirrorRows) {
		if (!props.isEnabled()) {
			return;
		}
		String db = props.getDatabase();
		String query =
				"INSERT INTO "
						+ db
						+ ".mirror_message_tail (lag_at_scrape, role, cluster, topic, partition, offset, kafka_ts_ms, message_key, value) FORMAT TabSeparated";

		List<String> lines = new ArrayList<>();
		for (TopicTailRecord r : sourceRows) {
			lines.add(
					row(
							lag.lagMessages(),
							"source",
							lag.sourceCluster(),
							lag.sourceTopic(),
							r));
		}
		for (TopicTailRecord r : mirrorRows) {
			lines.add(
					row(
							lag.lagMessages(),
							"mirror",
							lag.mirrorCluster(),
							lag.mirrorTopic(),
							r));
		}
		if (lines.isEmpty()) {
			return;
		}

		String body = String.join("\n", lines) + "\n";
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
					.body(body)
					.retrieve()
					.toBodilessEntity();
		} catch (Exception e) {
			log.warn("clickhouse mirror_message_tail insert failed: {}", e.toString());
		}
	}

	private static String row(
			long lagAtScrape,
			String role,
			String cluster,
			String topic,
			TopicTailRecord r) {
		return String.format(
				Locale.ROOT,
				"%d\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s",
				lagAtScrape,
				tsvEsc(role),
				tsvEsc(cluster),
				tsvEsc(topic),
				r.partition(),
				r.offset(),
				r.timestampMs(),
				tsvEsc(r.key()),
				tsvEsc(r.value()));
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
