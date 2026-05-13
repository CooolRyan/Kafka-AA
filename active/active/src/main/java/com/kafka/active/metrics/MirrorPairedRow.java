package com.kafka.active.metrics;

/**
 * 동일 {@code id}(JSON 페이로드) 기준으로 소스 토픽과 미러 토픽 tail 레코드를 한 줄로 묶은 행.
 */
public record MirrorPairedRow(
		String messageId,
		long sentAtMs,
		TopicTailRecord source,
		TopicTailRecord mirror,
		Long deltaKafkaTimestampMs) {}
