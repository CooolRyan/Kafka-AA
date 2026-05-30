package com.kafka.active.failover;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppKafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class FailoverDeliveryTracker {

	private final ObjectMapper objectMapper;
	private final AppKafkaProperties kafkaProperties;
	private final ClickHouseFailoverSink clickHouse;
	private volatile FailoverActiveSession session;

	public FailoverDeliveryTracker(
			ObjectMapper objectMapper,
			AppKafkaProperties kafkaProperties,
			ClickHouseFailoverSink clickHouse) {
		this.objectMapper = objectMapper;
		this.kafkaProperties = kafkaProperties;
		this.clickHouse = clickHouse;
	}

	void bindSession(FailoverActiveSession active) {
		this.session = active;
	}

	void clearSession() {
		this.session = null;
	}

	FailoverActiveSession session() {
		return session;
	}

	public void onProduced(FailoverProduceResult result) {
		FailoverActiveSession s = session;
		if (s == null || !s.runId().equals(parseTestRunId(result.payload()))) {
			return;
		}
		s.recordProduced(result.messageId());
		clickHouse.insertProduced(
				s.runId(),
				result.messageId(),
				result.produceCluster(),
				kafkaProperties.getProducerTopic(),
				result.partition(),
				result.offset(),
				result.sentAtMs(),
				result.payload());
	}

	public void onDedupSkipped(
			String consumerRole,
			String consumeCluster,
			ConsumerRecord<String, String> record,
			String messageId) {
		FailoverActiveSession s = session;
		if (s == null) {
			return;
		}
		ParsedTestMessage msg = parse(record.value());
		if (msg != null && s.runId().equals(msg.testRunId())) {
			s.recordDedupSkipped();
		}
		clickHouse.insertDedup(
				s == null ? "" : s.runId(),
				messageId,
				consumerRole,
				consumeCluster,
				record.topic(),
				record.partition(),
				record.offset(),
				"skipped_duplicate",
				record.value());
	}

	public void onConsumed(String consumerRole, String consumeCluster, ConsumerRecord<String, String> record) {
		FailoverActiveSession s = session;
		if (s == null || record.value() == null) {
			return;
		}
		ParsedTestMessage msg = parse(record.value());
		if (msg == null || !s.runId().equals(msg.testRunId())) {
			return;
		}
		int seq = s.recordConsumed(msg.messageId());
		boolean dup = seq > 1;
		clickHouse.insertConsumed(
				s.runId(),
				msg.messageId(),
				consumerRole,
				consumeCluster,
				record.topic(),
				record.partition(),
				record.offset(),
				seq,
				dup,
				record.value());
	}

	private ParsedTestMessage parse(String json) {
		try {
			JsonNode n = objectMapper.readTree(json);
			String testRunId = text(n, "testRunId");
			String id = text(n, "id");
			if (testRunId == null || testRunId.isBlank() || id == null || id.isBlank()) {
				return null;
			}
			return new ParsedTestMessage(testRunId, id);
		} catch (Exception ignored) {
			return null;
		}
	}

	private String parseTestRunId(String json) {
		ParsedTestMessage m = parse(json);
		return m == null ? null : m.testRunId();
	}

	private static String text(JsonNode n, String field) {
		JsonNode v = n.get(field);
		if (v == null || v.isNull()) {
			return null;
		}
		return v.asText();
	}

	private record ParsedTestMessage(String testRunId, String messageId) {}
}
