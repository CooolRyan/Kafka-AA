package com.kafka.active.failover;

import java.util.List;

public record MirrorBacklogCheckResult(
		String runId,
		String phase,
		long primaryCommittedLagSum,
		long mirrorLagMessages,
		int pendingTestIdCount,
		int pendingMirroredCount,
		int pendingMissingOnMirrorCount,
		int sourceUnreadSampleCount,
		int sourceUnreadMirroredCount,
		int sourceUnreadMissingOnMirrorCount,
		List<String> missingSampleIds,
		List<String> sourceUnreadMissingSampleIds) {}
