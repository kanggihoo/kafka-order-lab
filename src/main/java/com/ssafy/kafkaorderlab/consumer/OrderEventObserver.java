package com.ssafy.kafkaorderlab.consumer;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 주문 이벤트를 소비하고 Kafka 메타데이터를 로그로 남긴다.
 */
@Component
@Slf4j
class OrderEventObserver {

	private final OrderCreatedEventValidator validator;

	OrderEventObserver(OrderCreatedEventValidator validator) {
		this.validator = validator;
	}

	@KafkaListener(
		topics = "${app.kafka.topics.order-events}",
		groupId = "${app.kafka.consumer.order-observer.group-id}",
		concurrency = "${app.kafka.consumer.order-observer.concurrency}",
		containerFactory = "orderCreatedListenerContainerFactory"
	)
	void observe(ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>> record) {
		validator.validate(record.value());
		EventEnvelope<OrderCreatedPayload> event = record.value();
		log.info("order event observed: eventId={}, eventType={}, eventVersion={}, key={}, topic={}, partition={}, offset={}",
			event.eventId(), event.eventType(), event.eventVersion(), record.key(), record.topic(), record.partition(), record.offset());
	}
}
