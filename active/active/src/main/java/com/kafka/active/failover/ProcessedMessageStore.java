package com.kafka.active.failover;

import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * At-least-once 위에 올리는 앱 레벨 멱등: 동일 {@code id} 는 한 번만 비즈니스 처리.
 * (Kafka EOS / transactional 이 아닌 effectively-once 패턴)
 */
@Component
@Profile("!test")
public class ProcessedMessageStore {

	private final ConcurrentHashMap<String, Long> processedAtMs = new ConcurrentHashMap<>();

	public boolean alreadyProcessed(String messageId) {
		return messageId != null && processedAtMs.containsKey(messageId);
	}

	public boolean markProcessedIfAbsent(String messageId, long atMs) {
		if (messageId == null || messageId.isBlank()) {
			return true;
		}
		return processedAtMs.putIfAbsent(messageId, atMs) == null;
	}

	public int size() {
		return processedAtMs.size();
	}

	public void clear() {
		processedAtMs.clear();
	}
}
