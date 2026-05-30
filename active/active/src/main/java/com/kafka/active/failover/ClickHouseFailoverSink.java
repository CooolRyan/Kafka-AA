package com.kafka.active.failover;

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
public class ClickHouseFailoverSink {

	private static final Logger log = LoggerFactory.getLogger(ClickHouseFailoverSink.class);

	private final AppClickHouseProperties props;
	private final RestClient restClient;

	public ClickHouseFailoverSink(AppClickHouseProperties props, RestClient.Builder restClientBuilder) {
		this.props = props;
		this.restClient = restClientBuilder.build();
	}

	public void insertTestRun(
			String runId,
			String status,
			String produceCluster,
			String topic,
			String groupId,
			String notes) {
		insertJsonEachRow(
				"failover_test_run",
				"{\"run_id\":\"%s\",\"status\":\"%s\",\"produce_target_cluster\":\"%s\",\"consumer_topic\":\"%s\",\"consumer_group\":\"%s\",\"notes\":\"%s\"}"
						.formatted(
								esc(runId),
								esc(status),
								esc(produceCluster),
								esc(topic),
								esc(groupId),
								esc(notes)));
	}

	public void insertControlEvent(
			String runId, String eventType, String consumerRole, String detail) {
		insertJsonEachRow(
				"failover_control_event",
				"{\"run_id\":\"%s\",\"event_type\":\"%s\",\"consumer_role\":\"%s\",\"detail\":\"%s\"}"
						.formatted(esc(runId), esc(eventType), esc(consumerRole), esc(detail)));
	}

	public void insertProduced(
			String runId,
			String messageId,
			String cluster,
			String topic,
			int partition,
			long offset,
			long sentAtMs,
			String payload) {
		insertJsonEachRow(
				"failover_message_produced",
				"{\"run_id\":\"%s\",\"message_id\":\"%s\",\"produce_cluster\":\"%s\",\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"sent_at_ms\":%d,\"payload\":\"%s\"}"
						.formatted(
								esc(runId),
								esc(messageId),
								esc(cluster),
								esc(topic),
								partition,
								offset,
								sentAtMs,
								esc(payload)));
	}

	public void insertConsumed(
			String runId,
			String messageId,
			String consumerRole,
			String cluster,
			String topic,
			int partition,
			long offset,
			int consumeSeq,
			boolean duplicate,
			String payload) {
		insertJsonEachRow(
				"failover_message_consumed",
				"{\"run_id\":\"%s\",\"message_id\":\"%s\",\"consumer_role\":\"%s\",\"consume_cluster\":\"%s\",\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"consume_seq\":%d,\"is_duplicate\":%d,\"payload\":\"%s\"}"
						.formatted(
								esc(runId),
								esc(messageId),
								esc(consumerRole),
								esc(cluster),
								esc(topic),
								partition,
								offset,
								consumeSeq,
								duplicate ? 1 : 0,
								esc(payload)));
	}

	public void insertDedup(
			String runId,
			String messageId,
			String consumerRole,
			String cluster,
			String topic,
			int partition,
			long offset,
			String action,
			String payload) {
		insertJsonEachRow(
				"failover_message_dedup",
				"{\"run_id\":\"%s\",\"message_id\":\"%s\",\"consumer_role\":\"%s\",\"consume_cluster\":\"%s\",\"topic\":\"%s\",\"partition\":%d,\"offset\":%d,\"action\":\"%s\",\"payload\":\"%s\"}"
						.formatted(
								esc(runId),
								esc(messageId),
								esc(consumerRole),
								esc(cluster),
								esc(topic),
								partition,
								offset,
								esc(action),
								esc(payload)));
	}

	public void insertMirrorBacklogCheck(
			String runId,
			String phase,
			String messageId,
			String mirrorStatus,
			long primaryCommittedLag,
			long mirrorLag,
			int mirrorPartition,
			long mirrorOffset) {
		insertJsonEachRow(
				"failover_mirror_backlog_check",
				"{\"run_id\":\"%s\",\"phase\":\"%s\",\"message_id\":\"%s\",\"mirror_status\":\"%s\",\"primary_committed_lag_sum\":%d,\"mirror_lag_messages\":%d,\"mirror_partition\":%d,\"mirror_offset\":%d}"
						.formatted(
								esc(runId),
								esc(phase),
								esc(messageId),
								esc(mirrorStatus),
								primaryCommittedLag,
								mirrorLag,
								mirrorPartition,
								mirrorOffset));
	}

	public void insertSummary(FailoverTestReport report) {
		insertJsonEachRow(
				"failover_test_summary",
				"{\"run_id\":\"%s\",\"status\":\"%s\",\"produced_count\":%d,\"consumed_events\":%d,\"consumed_unique\":%d,\"duplicate_consume_count\":%d,\"dedup_skipped_count\":%d,\"missing_count\":%d,\"primary_committed_lag_at_failover\":%d,\"mirror_lag_at_failover\":%d,\"pending_unread_at_failover\":%d,\"missing_on_mirror_at_failover\":%d,\"active_consumer_role\":\"%s\",\"notes\":\"%s\"}"
						.formatted(
								esc(report.runId()),
								esc(report.status()),
								report.producedCount(),
								report.consumedEvents(),
								report.consumedUnique(),
								report.duplicateConsumeCount(),
								report.dedupSkippedCount(),
								report.missingCount(),
								report.primaryCommittedLagAtFailover(),
								report.mirrorLagAtFailover(),
								report.pendingUnreadAtFailover(),
								report.missingOnMirrorAtFailover(),
								esc(report.activeConsumerRole()),
								esc("missingSample="
										+ report.missingSampleIds().size()
										+ ",mirrorMissing="
										+ report.missingOnMirrorSampleIds().size())));
	}

	private void insertJsonEachRow(String table, String jsonLine) {
		if (!props.isEnabled()) {
			return;
		}
		String db = props.getDatabase();
		String query = "INSERT INTO " + db + "." + table + " FORMAT JSONEachRow";
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
					.body(jsonLine + "\n")
					.retrieve()
					.toBodilessEntity();
		} catch (Exception e) {
			log.warn("clickhouse {} insert failed: {}", table, e.toString());
		}
	}

	private static String esc(String v) {
		if (v == null) {
			return "";
		}
		return v.replace("\\", "\\\\")
				.replace("\"", "\\\"")
				.replace("\n", "\\n")
				.replace("\r", "\\r")
				.replace("\t", "\\t");
	}
}
