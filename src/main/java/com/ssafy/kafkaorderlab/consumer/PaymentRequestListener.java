package com.ssafy.kafkaorderlab.consumer;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import com.ssafy.kafkaorderlab.service.PaymentEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * `payment-service` consumer group으로 주문 생성 이벤트를 소비해 결제 요청 이벤트 발행을 유발한다.
 */
@Component
@Slf4j
class PaymentRequestListener {

	private final OrderCreatedEventValidator validator;
	private final PaymentEventService paymentEventService;

	PaymentRequestListener(OrderCreatedEventValidator validator, PaymentEventService paymentEventService) {
		this.validator = validator;
		this.paymentEventService = paymentEventService;
	}

	@KafkaListener(
		topics = "${app.kafka.topics.order-events}",
		groupId = "${app.kafka.consumer.payment-service.group-id}",
		concurrency = "${app.kafka.consumer.payment-service.concurrency}",
		containerFactory = "orderCreatedListenerContainerFactory"
	)
	void requestPayment(ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>> record) {
		validator.validate(record.value());
		OrderCreatedPayload payload = record.value().payload();
		log.info("order created consumed by payment-service: key={}, topic={}, partition={}, offset={}",
			record.key(), record.topic(), record.partition(), record.offset());
		paymentEventService.publishPaymentRequested(payload.orderId(), payload.amount());
	}
}
