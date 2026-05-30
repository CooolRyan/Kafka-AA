package com.kafka.active.failover;

public record FailoverProduceResult(
		String messageId,
		String produceCluster,
		int partition,
		long offset,
		long sentAtMs,
		String payload) {}
