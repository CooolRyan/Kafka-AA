package com.kafka.active.metrics;

public record MirrorLagSnapshot(
		long lagMessages,
		long sourceHighWatermark,
		long mirrorHighWatermark,
		String sourceCluster,
		String mirrorCluster,
		String sourceTopic,
		String mirrorTopic) {}
