package com.kafka.active.metrics;

import com.kafka.active.config.AppClickHouseProperties;
import java.net.URI;
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
public class ClickHouseMirrorLagSink {

	private static final Logger log = LoggerFactory.getLogger(ClickHouseMirrorLagSink.class);

	private final AppClickHouseProperties props;
	private final RestClient restClient;

	public ClickHouseMirrorLagSink(AppClickHouseProperties props, RestClient.Builder restClientBuilder) {
		this.props = props;
		this.restClient = restClientBuilder.build();
	}

	public void write(MirrorLagSnapshot s) {
		if (!props.isEnabled()) {
			return;
		}
		String db = props.getDatabase();
		String query =
				"INSERT INTO "
						+ db
						+ ".mirror_lag (lag_messages, source_hwm, mirror_hwm, source_cluster, mirror_cluster, source_topic, mirror_topic) FORMAT JSONEachRow";
		String row =
				String.format(
						Locale.ROOT,
						"{\"lag_messages\":%d,\"source_hwm\":%d,\"mirror_hwm\":%d,\"source_cluster\":\"%s\",\"mirror_cluster\":\"%s\",\"source_topic\":\"%s\",\"mirror_topic\":\"%s\"}",
						s.lagMessages(),
						s.sourceHighWatermark(),
						s.mirrorHighWatermark(),
						jsonString(s.sourceCluster()),
						jsonString(s.mirrorCluster()),
						jsonString(s.sourceTopic()),
						jsonString(s.mirrorTopic()));

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
					.body(row + "\n")
					.retrieve()
					.toBodilessEntity();
		} catch (Exception e) {
			log.warn("clickhouse insert failed: {}", e.toString());
		}
	}

	private static String jsonString(String v) {
		if (v == null) {
			return "";
		}
		return v.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
