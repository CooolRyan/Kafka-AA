package com.kafka.active.metrics;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class MirrorReplicationLagService {

	private final AppKafkaProperties kafkaProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;

	public MirrorReplicationLagService(
			AppKafkaProperties kafkaProperties, AppMirrorMetricsProperties mirrorMetricsProperties) {
		this.kafkaProperties = kafkaProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
	}

	/**
	 * 파티션별 max(0, sourceLatest - mirrorLatest) 합으로 “미러가 뒤처진 메시지 수” 근사치를 계산합니다.
	 * (MM2/Connect 설정·토픽명이 위 프로퍼티와 일치할 때 의미가 있습니다.)
	 */
	public MirrorLagSnapshot compute()
			throws ExecutionException, InterruptedException {
		String srcKey = mirrorMetricsProperties.getSourceCluster().trim().toUpperCase();
		String mirKey = mirrorMetricsProperties.getMirrorCluster().trim().toUpperCase();
		String srcTopic = mirrorMetricsProperties.getSourceTopic();
		String mirTopic = mirrorMetricsProperties.resolvedMirrorTopic();

		String srcBoot = kafkaProperties.bootstrapFor(srcKey);
		String mirBoot = kafkaProperties.bootstrapFor(mirKey);

		Map<Integer, Long> src = TopicEndOffsets.latestPerPartition(srcBoot, srcTopic);
		Map<Integer, Long> mir = TopicEndOffsets.latestPerPartition(mirBoot, mirTopic);

		long srcSum = 0;
		long mirSum = 0;
		long lag = 0;
		for (Integer p : src.keySet()) {
			if (!mir.containsKey(p)) {
				continue;
			}
			long s = src.get(p);
			long m = mir.get(p);
			srcSum += s;
			mirSum += m;
			lag += Math.max(0L, s - m);
		}
		return new MirrorLagSnapshot(lag, srcSum, mirSum, srcKey, mirKey, srcTopic, mirTopic);
	}
}
