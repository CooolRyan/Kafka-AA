package com.kafka.active.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** committed offset ~ log end 구간의 메시지를 tail 샘플로 근사 (전체 로그 스캔 아님). */
public final class TopicUnreadScanner {

	private TopicUnreadScanner() {}

	public static TopicUnreadSnapshot scan(
			String bootstrap, String topic, String groupId, int maxUnreadSample)
			throws ExecutionException, InterruptedException {
		Map<Integer, Long> end = TopicEndOffsets.latestPerPartition(bootstrap, topic);
		Map<Integer, Long> committed =
				ConsumerGroupOffsets.committedPerPartition(bootstrap, groupId, topic);

		long lagSum = 0;
		for (var e : end.entrySet()) {
			int p = e.getKey();
			lagSum += Math.max(0L, e.getValue() - committed.getOrDefault(p, 0L));
		}

		int tailLimit = (int) Math.min(500, Math.max(50, lagSum + 20));
		List<TopicTailRecord> tail = TopicRecentTailReader.readTail(bootstrap, topic, tailLimit);

		List<TopicTailRecord> unread = new ArrayList<>();
		for (TopicTailRecord r : tail) {
			long comm = committed.getOrDefault(r.partition(), 0L);
			if (r.offset() >= comm) {
				unread.add(r);
			}
		}
		if (unread.size() > maxUnreadSample) {
			unread = new ArrayList<>(unread.subList(0, maxUnreadSample));
		}
		return new TopicUnreadSnapshot(lagSum, committed, end, unread);
	}
}
