package com.kafka.active.metrics;

import java.util.List;

public record MirrorCompareDto(
		MirrorLagSnapshot lag,
		String sourceBootstrap,
		String mirrorBootstrap,
		List<TopicTailRecord> sourceMessages,
		List<TopicTailRecord> mirrorMessages) {}
