package com.kafka.active.metrics;

public record ParsedMirrorMessage(
		String id,
		String value,
		long sentAtMs,
		TopicTailRecord record) {}
