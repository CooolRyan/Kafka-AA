package com.kafka.active.failover;

import java.util.List;

public record MirrorBacklogCheckResult(
		String runId,
		String phase,
		long primaryCommittedLagSum,
		long mirrorLagMessages,
		int uncheckedUnreadCount,
		int mirroredCount,
		int missingOnMirrorCount,
		List<String> missingSampleIds) {}
