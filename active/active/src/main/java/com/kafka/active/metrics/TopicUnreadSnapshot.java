package com.kafka.active.metrics;

import java.util.List;
import java.util.Map;

/** Primary 토픽에서 committed 이후(미처리 구간)로 보이는 레코드 스냅샷 */
public record TopicUnreadSnapshot(
		long committedLagSum,
		Map<Integer, Long> committedPerPartition,
		Map<Integer, Long> endPerPartition,
		List<TopicTailRecord> unreadRecords) {}
