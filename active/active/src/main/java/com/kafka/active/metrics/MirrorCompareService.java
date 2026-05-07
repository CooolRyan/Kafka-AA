package com.kafka.active.metrics;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class MirrorCompareService {

	private final MirrorReplicationLagService lagService;
	private final AppKafkaProperties kafkaProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;

	public MirrorCompareService(
			MirrorReplicationLagService lagService,
			AppKafkaProperties kafkaProperties,
			AppMirrorMetricsProperties mirrorMetricsProperties) {
		this.lagService = lagService;
		this.kafkaProperties = kafkaProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
	}

	public MirrorCompareDto compare(int limit) throws ExecutionException, InterruptedException {
		MirrorLagSnapshot lag = lagService.compute();
		String srcKey = mirrorMetricsProperties.getSourceCluster().trim().toUpperCase();
		String mirKey = mirrorMetricsProperties.getMirrorCluster().trim().toUpperCase();
		String srcTopic = mirrorMetricsProperties.getSourceTopic();
		String mirTopic = mirrorMetricsProperties.resolvedMirrorTopic();
		String srcBoot = kafkaProperties.bootstrapFor(srcKey);
		String mirBoot = kafkaProperties.bootstrapFor(mirKey);
		List<TopicTailRecord> src = TopicRecentTailReader.readTail(srcBoot, srcTopic, limit);
		List<TopicTailRecord> mir = TopicRecentTailReader.readTail(mirBoot, mirTopic, limit);
		return new MirrorCompareDto(lag, srcBoot, mirBoot, src, mir);
	}
}
