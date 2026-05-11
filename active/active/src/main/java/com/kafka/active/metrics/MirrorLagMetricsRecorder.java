package com.kafka.active.metrics;

import com.kafka.active.config.AppClickHouseProperties;
import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.config.AppMirrorMetricsProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Profile("!test")
public class MirrorLagMetricsRecorder {

	private static final Logger log = LoggerFactory.getLogger(MirrorLagMetricsRecorder.class);

	private final MirrorReplicationLagService lagService;
	private final ClickHouseMirrorLagSink clickHouseMirrorLagSink;
	private final ClickHouseMirrorTailSink clickHouseMirrorTailSink;
	private final ClickHouseMirrorCompareSink clickHouseMirrorCompareSink;
	private final MirrorMessageCompareService mirrorMessageCompareService;
	private final AppKafkaProperties kafkaProperties;
	private final AppClickHouseProperties clickHouseProperties;
	private final AppMirrorMetricsProperties mirrorMetricsProperties;
	private final MeterRegistry meterRegistry;
	private final AtomicLong lagMessages = new AtomicLong(-1);
	private final AtomicLong sourceHwm = new AtomicLong(-1);
	private final AtomicLong mirrorHwm = new AtomicLong(-1);

	public MirrorLagMetricsRecorder(
			MirrorReplicationLagService lagService,
			ClickHouseMirrorLagSink clickHouseMirrorLagSink,
			ClickHouseMirrorTailSink clickHouseMirrorTailSink,
			ClickHouseMirrorCompareSink clickHouseMirrorCompareSink,
			MirrorMessageCompareService mirrorMessageCompareService,
			AppKafkaProperties kafkaProperties,
			AppClickHouseProperties clickHouseProperties,
			AppMirrorMetricsProperties mirrorMetricsProperties,
			MeterRegistry meterRegistry) {
		this.lagService = lagService;
		this.clickHouseMirrorLagSink = clickHouseMirrorLagSink;
		this.clickHouseMirrorTailSink = clickHouseMirrorTailSink;
		this.clickHouseMirrorCompareSink = clickHouseMirrorCompareSink;
		this.mirrorMessageCompareService = mirrorMessageCompareService;
		this.kafkaProperties = kafkaProperties;
		this.clickHouseProperties = clickHouseProperties;
		this.mirrorMetricsProperties = mirrorMetricsProperties;
		this.meterRegistry = meterRegistry;
	}

	@PostConstruct
	void registerGauges() {
		Gauge.builder("kafka.mirror.lag.messages", lagMessages, AtomicLong::get)
				.description("Approximate mirror backlog: sum per partition of max(0, source_latest - mirror_latest)")
				.register(meterRegistry);
		Gauge.builder("kafka.mirror.source.highwater", sourceHwm, AtomicLong::get)
				.description("Sum of latest offsets on source topic")
				.register(meterRegistry);
		Gauge.builder("kafka.mirror.replica.highwater", mirrorHwm, AtomicLong::get)
				.description("Sum of latest offsets on mirror topic")
				.register(meterRegistry);
	}

	@Scheduled(fixedDelayString = "${app.mirror-metrics.interval-ms:15000}")
	public void record() {
		if (!mirrorMetricsProperties.isEnabled()) {
			return;
		}
		try {
			MirrorLagSnapshot snap = lagService.compute();
			lagMessages.set(snap.lagMessages());
			sourceHwm.set(snap.sourceHighWatermark());
			mirrorHwm.set(snap.mirrorHighWatermark());
			clickHouseMirrorLagSink.write(snap);
			if (mirrorMetricsProperties.isTailToClickhouse() && clickHouseProperties.isEnabled()) {
				int lim = mirrorMetricsProperties.getTailSampleLimit();
				String srcKey = mirrorMetricsProperties.getSourceCluster().trim().toUpperCase();
				String mirKey = mirrorMetricsProperties.getMirrorCluster().trim().toUpperCase();
				String srcBoot = kafkaProperties.bootstrapFor(srcKey);
				String mirBoot = kafkaProperties.bootstrapFor(mirKey);
				var src = TopicRecentTailReader.readTail(srcBoot, snap.sourceTopic(), lim);
				var mir = TopicRecentTailReader.readTail(mirBoot, snap.mirrorTopic(), lim);
				clickHouseMirrorTailSink.write(snap, src, mir);
				clickHouseMirrorCompareSink.write(
						mirrorMessageCompareService.observeAndCompare(snap, src, mir));
			}
		} catch (Exception e) {
			log.warn("mirror lag scrape failed: {}", e.toString());
		}
	}
}
