package com.kafka.active.config;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {

	private final Map<String, Cluster> clusters = new HashMap<>();
	private String producerTopic = "aa-demo-events";
	private final Consumer consumer = new Consumer();
	private long failoverCheckIntervalMs = 15_000L;

	public Map<String, Cluster> getClusters() {
		return clusters;
	}

	public String getProducerTopic() {
		return producerTopic;
	}

	public void setProducerTopic(String producerTopic) {
		this.producerTopic = producerTopic;
	}

	public Consumer getConsumer() {
		return consumer;
	}

	public long getFailoverCheckIntervalMs() {
		return failoverCheckIntervalMs;
	}

	public void setFailoverCheckIntervalMs(long failoverCheckIntervalMs) {
		this.failoverCheckIntervalMs = failoverCheckIntervalMs;
	}

	public String bootstrapFor(String clusterKey) {
		Cluster c = clusters.get(clusterKey);
		if (c == null || c.getBootstrapServers() == null || c.getBootstrapServers().isBlank()) {
			throw new IllegalStateException("Unknown or empty cluster: " + clusterKey);
		}
		return c.getBootstrapServers();
	}

	public static class Cluster {
		private String bootstrapServers;

		public String getBootstrapServers() {
			return bootstrapServers;
		}

		public void setBootstrapServers(String bootstrapServers) {
			this.bootstrapServers = bootstrapServers;
		}
	}

	/**
	 * dual-listener: Primary/Standby listener 수동·watchdog 전환 (데모용).
	 * proxy: bootstrap 은 HAProxy 단일 주소, 브로커 장애 시 프록시가 backup 으로 TCP 전환.
	 */
	private String consumerMode = "dual-listener";

	/** consumerMode=proxy 일 때 Spring consumer 가 붙는 bootstrap (예: 127.0.0.1:19092) */
	private String proxyBootstrapServers = "127.0.0.1:19092";

	public String getConsumerMode() {
		return consumerMode;
	}

	public void setConsumerMode(String consumerMode) {
		this.consumerMode = consumerMode;
	}

	public boolean isProxyConsumerMode() {
		return "proxy".equalsIgnoreCase(consumerMode == null ? "" : consumerMode.trim());
	}

	public String getProxyBootstrapServers() {
		return proxyBootstrapServers;
	}

	public void setProxyBootstrapServers(String proxyBootstrapServers) {
		this.proxyBootstrapServers = proxyBootstrapServers;
	}

	public static class Consumer {
		/** 페일오버 전에 메시지를 100% 소비할 클러스터 키 (예: A) */
		private String primary = "A";
		/** Primary 가 consume 하는 소스 토픽 (클러스터 A) */
		private String topic = "aa-demo-events";
		/**
		 * Failover(Standby/프록시→B) 시 MM2 미러 토픽. 비우면 {@code {primary}.aa-demo-events} (예: A.aa-demo-events)
		 */
		private String mirrorTopic = "";
		private String groupId = "aa-demo-app";
		/** at-least-once + 동일 JSON id 재처리 스킵 (effectively-once) */
		private boolean idempotencyEnabled = true;

		public String getPrimary() {
			return primary;
		}

		public void setPrimary(String primary) {
			this.primary = primary;
		}

		public String getTopic() {
			return topic;
		}

		public void setTopic(String topic) {
			this.topic = topic;
		}

		public String getGroupId() {
			return groupId;
		}

		public void setGroupId(String groupId) {
			this.groupId = groupId;
		}

		public String getMirrorTopic() {
			return mirrorTopic;
		}

		public void setMirrorTopic(String mirrorTopic) {
			this.mirrorTopic = mirrorTopic;
		}

		public String resolvedMirrorTopic(String primaryClusterKey) {
			if (mirrorTopic != null && !mirrorTopic.isBlank()) {
				return mirrorTopic;
			}
			return primaryClusterKey.trim().toUpperCase() + "." + topic;
		}

		public boolean isIdempotencyEnabled() {
			return idempotencyEnabled;
		}

		public void setIdempotencyEnabled(boolean idempotencyEnabled) {
			this.idempotencyEnabled = idempotencyEnabled;
		}
	}
}
