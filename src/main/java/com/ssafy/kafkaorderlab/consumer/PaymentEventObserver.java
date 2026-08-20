package com.ssafy.kafkaorderlab.consumer;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 결제 요청 이벤트를 소비하고 Kafka 메타데이터를 로그로 남기는 가짜 결제 observer다.
 */
@Component
@Slf4j
class PaymentEventObserver {

	private final PaymentRequestedEventValidator validator;

	PaymentEventObserver(PaymentRequestedEventValidator validator) {
		this.validator = validator;
	}

	@KafkaListener(
		topics = "${app.kafka.topics.payment-events}",
		groupId = "${app.kafka.consumer.payment-observer.group-id}",
		concurrency = "${app.kafka.consumer.payment-observer.concurrency}",
		containerFactory = "paymentRequestedListenerContainerFactory"
	)
	void observe(ConsumerRecord<String, EventEnvelope<PaymentRequestedPayload>> record) {
		validator.validate(record.value());
		EventEnvelope<PaymentRequestedPayload> event = record.value();
		log.info("payment event observed: eventId={}, eventType={}, eventVersion={}, key={}, topic={}, partition={}, offset={}",
			event.eventId(), event.eventType(), event.eventVersion(), record.key(), record.topic(), record.partition(), record.offset());
	}
}
