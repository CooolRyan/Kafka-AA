package com.kafka.active.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.mirror-metrics")
public class AppMirrorMetricsProperties {

	private boolean enabled = true;
	private long intervalMs = 15_000L;
	/** 소스(원본) 토픽이 있는 클러스터 키 */
	private String sourceCluster = "A";
	/** 미러 토픽이 있는 클러스터 키 */
	private String mirrorCluster = "B";
	private String sourceTopic = "aa-demo-events";
	/**
	 * 미러 쪽 토픽명. 비우면 소스와 동일 이름(Connect 등 동일 토픽명 복제).
	 * MM2는 보통 B에 {@code A.aa-demo-events} 형태 — application.yaml 에서 지정 권장.
	 */
	private String mirrorTopic = "";

	/** ClickHouse·대시보드용 tail 샘플 건수 (소스/미러 각각) */
	private int tailSampleLimit = 24;

	/** true 이면 mirror-metrics 스케줄마다 mirror_message_tail 에 적재 */
	private boolean tailToClickhouse = true;

	/** Kafka tail 이 비었을 때 compare UI 를 ClickHouse 스냅샷으로 채움 */
	private boolean compareFallbackClickhouse = true;

	/** compare ClickHouse fallback: 소스/미러 각각 ts DESC 최신 N건 (UI limit 미지정 시) */
	private int compareClickhouseRowLimit = 30;

	public boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}

	public long getIntervalMs() {
		return intervalMs;
	}

	public void setIntervalMs(long intervalMs) {
		this.intervalMs = intervalMs;
	}

	public String getSourceCluster() {
		return sourceCluster;
	}

	public void setSourceCluster(String sourceCluster) {
		this.sourceCluster = sourceCluster;
	}

	public String getMirrorCluster() {
		return mirrorCluster;
	}

	public void setMirrorCluster(String mirrorCluster) {
		this.mirrorCluster = mirrorCluster;
	}

	public String getSourceTopic() {
		return sourceTopic;
	}

	public void setSourceTopic(String sourceTopic) {
		this.sourceTopic = sourceTopic;
	}

	public String getMirrorTopic() {
		return mirrorTopic;
	}

	public void setMirrorTopic(String mirrorTopic) {
		this.mirrorTopic = mirrorTopic;
	}

	public String resolvedMirrorTopic() {
		if (mirrorTopic == null || mirrorTopic.isBlank()) {
			return sourceTopic;
		}
		return mirrorTopic;
	}

	public int getTailSampleLimit() {
		return tailSampleLimit;
	}

	public void setTailSampleLimit(int tailSampleLimit) {
		this.tailSampleLimit = tailSampleLimit;
	}

	public boolean isTailToClickhouse() {
		return tailToClickhouse;
	}

	public void setTailToClickhouse(boolean tailToClickhouse) {
		this.tailToClickhouse = tailToClickhouse;
	}

	public boolean isCompareFallbackClickhouse() {
		return compareFallbackClickhouse;
	}

	public void setCompareFallbackClickhouse(boolean compareFallbackClickhouse) {
		this.compareFallbackClickhouse = compareFallbackClickhouse;
	}

	public int getCompareClickhouseRowLimit() {
		return compareClickhouseRowLimit;
	}

	public void setCompareClickhouseRowLimit(int compareClickhouseRowLimit) {
		this.compareClickhouseRowLimit = compareClickhouseRowLimit;
	}
}
