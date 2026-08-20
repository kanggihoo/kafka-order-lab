package com.ssafy.kafkaorderlab.service;

import static com.ssafy.kafkaorderlab.event.PaymentRequestedEventContract.TYPE;
import static com.ssafy.kafkaorderlab.event.PaymentRequestedEventContract.VERSION_1;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.EventHeaders;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

/**
 * 주문 생성 이벤트에 대한 가짜 결제 요청 이벤트 발행을 담당한다.
 */
@Service
@Slf4j
public class PaymentEventService {

	private final KafkaTemplate<String, EventEnvelope<PaymentRequestedPayload>> kafkaTemplate;
	private final String paymentEventsTopic;

	public PaymentEventService(
		KafkaTemplate<String, EventEnvelope<PaymentRequestedPayload>> kafkaTemplate,
		@Value("${app.kafka.topics.payment-events}") String paymentEventsTopic
	) {
		this.kafkaTemplate = kafkaTemplate;
		this.paymentEventsTopic = paymentEventsTopic;
	}

	/**
	 * 결제 요청 이벤트를 `payment-events`에 발행한다. 원본 주문과 같은 `orderId`를 key로 사용해 주문별 순서를 유지한다.
	 *
	 * @param orderId 결제를 요청한 주문 식별자
	 * @param amount 결제 요청 금액
	 */
	public void publishPaymentRequested(String orderId, long amount) {
		EventEnvelope<PaymentRequestedPayload> event = new EventEnvelope<>(UUID.randomUUID(), TYPE, VERSION_1,
			Instant.now(), new PaymentRequestedPayload(orderId, amount));
		ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>> record = new ProducerRecord<>(
			paymentEventsTopic, orderId, event);
		EventHeaders.addContractHeaders(record.headers(), event.eventType(), event.eventVersion());
		CompletableFuture<SendResult<String, EventEnvelope<PaymentRequestedPayload>>> future = kafkaTemplate.send(record);
		future.whenComplete((result, error) -> {
			if (error != null) {
				log.error("payment event publish failed: topic={}, key={}", paymentEventsTopic, orderId, error);
				return;
			}
			RecordMetadata metadata = result.getRecordMetadata();
			log.info("payment event published: eventId={}, key={}, topic={}, partition={}, offset={}", event.eventId(),
				orderId, metadata.topic(), metadata.partition(), metadata.offset());
		});
	}
}
