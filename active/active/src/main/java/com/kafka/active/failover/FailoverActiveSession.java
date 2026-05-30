package com.kafka.active.failover;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class FailoverActiveSession {

	private final String runId;
	private final String produceTargetCluster;
	private final String consumerTopic;
	private final String consumerGroup;
	private final long startedAtMs;
	private final Set<String> producedIds = ConcurrentHashMap.newKeySet();
	private final ConcurrentHashMap<String, AtomicInteger> consumeCounts = new ConcurrentHashMap<>();
	private final AtomicInteger consumedEvents = new AtomicInteger();
	private final AtomicInteger dedupSkipped = new AtomicInteger();
	private volatile MirrorBacklogCheckResult lastBacklogCheck;

	FailoverActiveSession(
			String runId,
			String produceTargetCluster,
			String consumerTopic,
			String consumerGroup,
			long startedAtMs) {
		this.runId = runId;
		this.produceTargetCluster = produceTargetCluster;
		this.consumerTopic = consumerTopic;
		this.consumerGroup = consumerGroup;
		this.startedAtMs = startedAtMs;
	}

	String runId() {
		return runId;
	}

	String produceTargetCluster() {
		return produceTargetCluster;
	}

	String consumerTopic() {
		return consumerTopic;
	}

	String consumerGroup() {
		return consumerGroup;
	}

	long startedAtMs() {
		return startedAtMs;
	}

	void recordProduced(String messageId) {
		producedIds.add(messageId);
	}

	int recordConsumed(String messageId) {
		consumedEvents.incrementAndGet();
		return consumeCounts.computeIfAbsent(messageId, k -> new AtomicInteger()).incrementAndGet();
	}

	void recordDedupSkipped() {
		dedupSkipped.incrementAndGet();
	}

	void setLastBacklogCheck(MirrorBacklogCheckResult check) {
		this.lastBacklogCheck = check;
	}

	MirrorBacklogCheckResult lastBacklogCheck() {
		return lastBacklogCheck;
	}

	Set<String> pendingMessageIds() {
		Set<String> pending = new HashSet<>();
		for (String id : producedIds) {
			AtomicInteger c = consumeCounts.get(id);
			if (c == null || c.get() == 0) {
				pending.add(id);
			}
		}
		return pending;
	}

	FailoverTestReport buildReport(String status, boolean onStandby, Long finishedAtMs) {
		int produced = producedIds.size();
		int events = consumedEvents.get();
		int unique = 0;
		int duplicates = 0;
		List<String> dupSamples = new ArrayList<>();
		for (var e : consumeCounts.entrySet()) {
			int c = e.getValue().get();
			if (c > 0) {
				unique++;
			}
			if (c > 1) {
				duplicates += c - 1;
				if (dupSamples.size() < 10) {
					dupSamples.add(e.getKey());
				}
			}
		}
		List<String> missing = new ArrayList<>();
		int missingCount = 0;
		for (String id : producedIds) {
			AtomicInteger c = consumeCounts.get(id);
			if (c == null || c.get() == 0) {
				missingCount++;
				if (missing.size() < 20) {
					missing.add(id);
				}
			}
		}
		String role = onStandby ? "STANDBY_MIRROR" : "PRIMARY";
		MirrorBacklogCheckResult bl = lastBacklogCheck;
		return new FailoverTestReport(
				runId,
				status,
				produceTargetCluster,
				consumerTopic,
				consumerGroup,
				onStandby,
				role,
				produced,
				events,
				unique,
				duplicates,
				dedupSkipped.get(),
				missingCount,
				missing,
				dupSamples,
				bl == null ? 0L : bl.primaryCommittedLagSum(),
				bl == null ? 0L : bl.mirrorLagMessages(),
				bl == null ? 0 : bl.uncheckedUnreadCount(),
				bl == null ? 0 : bl.missingOnMirrorCount(),
				bl == null ? List.of() : bl.missingSampleIds(),
				startedAtMs,
				finishedAtMs);
	}
}
