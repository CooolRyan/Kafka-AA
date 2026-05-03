package com.kafka.active.kafka;

import com.kafka.active.config.AppKafkaProperties;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service
@Profile("!test")
public class DualClusterTrafficProducer {

	private static final Logger log = LoggerFactory.getLogger(DualClusterTrafficProducer.class);

	private final KafkaTemplate<String, String> kafkaTemplateA;
	private final KafkaTemplate<String, String> kafkaTemplateB;
	private final AppKafkaProperties props;
	private final AtomicInteger seq = new AtomicInteger();

	public DualClusterTrafficProducer(
			@Qualifier("kafkaTemplateA") KafkaTemplate<String, String> kafkaTemplateA,
			@Qualifier("kafkaTemplateB") KafkaTemplate<String, String> kafkaTemplateB,
			AppKafkaProperties props) {
		this.kafkaTemplateA = kafkaTemplateA;
		this.kafkaTemplateB = kafkaTemplateB;
		this.props = props;
	}

	/** 라운드로빈으로 클러스터 A/B에 약 50:50 발행 */
	public void sendNext(String payload) {
		boolean toA = seq.getAndIncrement() % 2 == 0;
		var tpl = toA ? kafkaTemplateA : kafkaTemplateB;
		String cluster = toA ? "A" : "B";
		tpl.send(props.getProducerTopic(), payload)
				.whenComplete((r, e) -> {
					if (e != null) {
						log.warn("produce failed cluster={} topic={} err={}", cluster, props.getProducerTopic(), e.toString());
					} else {
						log.info("produced cluster={} partition={} offset={}", cluster, r.getRecordMetadata().partition(), r.getRecordMetadata().offset());
					}
				});
	}

	public void sendBatch(int count, String prefix) {
		for (int i = 0; i < count; i++) {
			sendNext(prefix + "-" + i);
		}
	}
}
