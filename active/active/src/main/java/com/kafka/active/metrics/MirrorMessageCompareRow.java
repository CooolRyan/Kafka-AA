package com.kafka.active.metrics;

public record MirrorMessageCompareRow(
		String messageId,
		String value,
		String sourceCluster,
		String mirrorCluster,
		String sourceTopic,
		String mirrorTopic,
		int sourcePartition,
		long sourceOffset,
		int mirrorPartition,
		long mirrorOffset,
		long sentAtMs,
		long sourceKafkaTsMs,
		long mirrorKafkaTsMs,
		long sourceSeenAtMs,
		long mirrorSeenAtMs,
		long replicationDelayMs,
		double replicationDelaySec,
		long endToEndDelayMs,
		double endToEndDelaySec,
		long kafkaTimestampDeltaMs) {}
