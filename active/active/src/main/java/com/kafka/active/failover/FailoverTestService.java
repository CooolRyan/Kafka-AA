package com.kafka.active.failover;

import com.kafka.active.config.AppKafkaProperties;
import com.kafka.active.kafka.ConsumerFailoverCoordinator;
import com.kafka.active.kafka.DualClusterTrafficProducer;
import java.time.Clock;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class FailoverTestService {

	private final FailoverDeliveryTracker tracker;
	private final ClickHouseFailoverSink clickHouse;
	private final DualClusterTrafficProducer producer;
	private final ObjectProvider<ConsumerFailoverCoordinator> coordinatorProvider;
	private final FailoverMirrorBacklogVerifier backlogVerifier;
	private final ProcessedMessageStore processedMessageStore;
	private final AppKafkaProperties kafkaProperties;
	private final Clock clock;

	public FailoverTestService(
			FailoverDeliveryTracker tracker,
			ClickHouseFailoverSink clickHouse,
			DualClusterTrafficProducer producer,
			ObjectProvider<ConsumerFailoverCoordinator> coordinatorProvider,
			FailoverMirrorBacklogVerifier backlogVerifier,
			ProcessedMessageStore processedMessageStore,
			AppKafkaProperties kafkaProperties) {
		this.tracker = tracker;
		this.clickHouse = clickHouse;
		this.producer = producer;
		this.coordinatorProvider = coordinatorProvider;
		this.backlogVerifier = backlogVerifier;
		this.processedMessageStore = processedMessageStore;
		this.kafkaProperties = kafkaProperties;
		this.clock = Clock.systemUTC();
	}

	public synchronized FailoverTestReport startRun(String produceCluster) {
		if (tracker.session() != null) {
			throw new IllegalStateException("active test run exists: " + tracker.session().runId());
		}
		String cluster =
				(produceCluster == null || produceCluster.isBlank())
						? kafkaProperties.getConsumer().getPrimary().trim().toUpperCase()
						: produceCluster.trim().toUpperCase();
		processedMessageStore.clear();
		String runId = UUID.randomUUID().toString();
		var session =
				new FailoverActiveSession(
						runId,
						cluster,
						kafkaProperties.getConsumer().getTopic(),
						kafkaProperties.getConsumer().getGroupId(),
						clock.millis());
		tracker.bindSession(session);
		clickHouse.insertTestRun(
				runId,
				"running",
				cluster,
				session.consumerTopic(),
				session.consumerGroup(),
				"failover delivery verification");
		recordControl(runId, "test_start", activeRole(), "produceTarget=" + cluster);
		return liveReport();
	}

	public synchronized List<String> produceForRun(int count, String prefix) {
		FailoverActiveSession session = requireSession();
		String p = (prefix == null || prefix.isBlank()) ? "fo-verify" : prefix.trim();
		List<FailoverProduceResult> results =
				producer.sendBatchForTest(session.produceTargetCluster(), count, p, session.runId());
		for (FailoverProduceResult r : results) {
			tracker.onProduced(r);
		}
		return results.stream().map(FailoverProduceResult::messageId).toList();
	}

	public synchronized FailoverTestReport failoverToStandby() {
		FailoverActiveSession session = requireSession();
		requireDualListenerMode("failover/standby");
		try {
			MirrorBacklogCheckResult check =
					backlogVerifier.verifyBeforeFailover(session, "pre_failover");
			session.setLastBacklogCheck(check);
			recordControl(
					session.runId(),
					"mirror_backlog_check",
					"PRIMARY",
					"pending="
							+ check.pendingTestIdCount()
							+ " pendingMiss="
							+ check.pendingMissingOnMirrorCount()
							+ " unreadSample="
							+ check.sourceUnreadSampleCount()
							+ " unreadMiss="
							+ check.sourceUnreadMissingOnMirrorCount());
		} catch (Exception e) {
			throw new IllegalStateException("mirror backlog check failed: " + e.getMessage(), e);
		}
		coordinatorProvider.getObject().failoverToStandby();
		recordControl(session.runId(), "failover_standby", "STANDBY_MIRROR", "manual failover");
		return liveReport();
	}

	public synchronized MirrorBacklogCheckResult checkMirrorBacklog() {
		FailoverActiveSession session = requireSession();
		try {
			MirrorBacklogCheckResult check =
					backlogVerifier.verifyBeforeFailover(session, "manual_check");
			session.setLastBacklogCheck(check);
			return check;
		} catch (Exception e) {
			throw new IllegalStateException("mirror backlog check failed: " + e.getMessage(), e);
		}
	}

	public synchronized FailoverTestReport failbackToPrimary() {
		requireSession();
		requireDualListenerMode("failback/primary");
		coordinatorProvider.getObject().failbackToPrimary();
		recordControl(requireSession().runId(), "failback_primary", "PRIMARY", "manual failback");
		return liveReport();
	}

	private void requireDualListenerMode(String action) {
		if (kafkaProperties.isProxyConsumerMode()) {
			throw new IllegalStateException(
					action
							+ " is disabled in consumer-mode=proxy — stop cluster A or HAProxy primary check instead");
		}
	}

	public synchronized FailoverTestReport finishRun() {
		FailoverActiveSession session = requireSession();
		FailoverTestReport report =
				session.buildReport("finished", isConsumingOnStandby(), clock.millis());
		clickHouse.insertSummary(report);
		clickHouse.insertTestRun(
				session.runId(),
				"finished",
				session.produceTargetCluster(),
				session.consumerTopic(),
				session.consumerGroup(),
				"produced="
						+ report.producedCount()
						+ " missing="
						+ report.missingCount()
						+ " dup="
						+ report.duplicateConsumeCount());
		recordControl(session.runId(), "test_finish", report.activeConsumerRole(), "finished");
		tracker.clearSession();
		return report;
	}

	public FailoverTestReport liveReport() {
		FailoverActiveSession session = tracker.session();
		if (session == null) {
			return null;
		}
		return session.buildReport("running", isConsumingOnStandby(), null);
	}

	public String activeRunId() {
		FailoverActiveSession s = tracker.session();
		return s == null ? null : s.runId();
	}

	private FailoverActiveSession requireSession() {
		FailoverActiveSession s = tracker.session();
		if (s == null) {
			throw new IllegalStateException("no active failover test — POST /api/failover/test/start first");
		}
		return s;
	}

	private void recordControl(String runId, String type, String role, String detail) {
		clickHouse.insertControlEvent(runId, type, role, detail);
	}

	private String activeRole() {
		if (kafkaProperties.isProxyConsumerMode()) {
			return "PROXY";
		}
		return isConsumingOnStandby() ? "STANDBY" : "PRIMARY";
	}

	private boolean isConsumingOnStandby() {
		ConsumerFailoverCoordinator c = coordinatorProvider.getIfAvailable();
		return c != null && c.isConsumingOnStandby();
	}
}
