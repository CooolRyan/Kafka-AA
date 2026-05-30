package com.kafka.active.failover;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.active.config.AppKafkaProperties;
import java.time.Clock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class IdempotentConsumeHandler {

	private static final Logger log = LoggerFactory.getLogger(IdempotentConsumeHandler.class);

	private final ProcessedMessageStore store;
	private final FailoverDeliveryTracker tracker;
	private final AppKafkaProperties kafkaProperties;
	private final ObjectMapper objectMapper;
	private final Clock clock;

	public IdempotentConsumeHandler(
			ProcessedMessageStore store,
			FailoverDeliveryTracker tracker,
			AppKafkaProperties kafkaProperties,
			ObjectMapper objectMapper) {
		this.store = store;
		this.tracker = tracker;
		this.kafkaProperties = kafkaProperties;
		this.objectMapper = objectMapper;
		this.clock = Clock.systemUTC();
	}

	public void consume(String consumerRole, String consumeCluster, ConsumerRecord<String, String> record) {
		String messageId = extractMessageId(record.value());
		if (!kafkaProperties.getConsumer().isIdempotencyEnabled()) {
			tracker.onConsumed(consumerRole, consumeCluster, record);
			log.info("[consume][{}] {}", consumerRole, record.value());
			return;
		}
		if (messageId != null && store.alreadyProcessed(messageId)) {
			tracker.onDedupSkipped(consumerRole, consumeCluster, record, messageId);
			log.warn(
					"[consume][{}][SKIP-DUP] id={} p={} offset={}",
					consumerRole,
					messageId,
					record.partition(),
					record.offset());
			return;
		}
		if (messageId != null) {
			store.markProcessedIfAbsent(messageId, clock.millis());
		}
		tracker.onConsumed(consumerRole, consumeCluster, record);
		log.info("[consume][{}] {}", consumerRole, record.value());
	}

	private String extractMessageId(String json) {
		if (json == null || json.isBlank()) {
			return null;
		}
		try {
			JsonNode n = objectMapper.readTree(json);
			if (n.hasNonNull("id") && n.get("id").isTextual()) {
				return n.get("id").asText();
			}
		} catch (Exception ignored) {
		}
		return null;
	}
}
