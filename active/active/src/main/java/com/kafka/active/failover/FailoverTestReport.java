package com.kafka.active.failover;

import java.util.List;

public record FailoverTestReport(
		String runId,
		String status,
		String produceTargetCluster,
		String consumerTopic,
		String consumerGroup,
		boolean consumingOnStandby,
		String activeConsumerRole,
		int producedCount,
		int consumedEvents,
		int consumedUnique,
		int duplicateConsumeCount,
		int dedupSkippedCount,
		int missingCount,
		List<String> missingSampleIds,
		List<String> duplicateSampleIds,
		long primaryCommittedLagAtFailover,
		long mirrorLagAtFailover,
		int pendingUnreadAtFailover,
		int missingOnMirrorAtFailover,
		List<String> missingOnMirrorSampleIds,
		long startedAtMs,
		Long finishedAtMs) {}
