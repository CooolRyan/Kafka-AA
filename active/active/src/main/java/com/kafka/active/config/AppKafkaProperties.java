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

	public static class Consumer {
		/** 페일오버 전에 메시지를 100% 소비할 클러스터 키 (예: A) */
		private String primary = "A";
		private String topic = "aa-demo-events";
		private String groupId = "aa-demo-app";

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
	}
}
