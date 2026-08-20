package com.ssafy.kafkaorderlab.service;

import static com.ssafy.kafkaorderlab.event.OrderCreatedEventContract.TYPE;
import static com.ssafy.kafkaorderlab.event.OrderCreatedEventContract.VERSION_1;
import static com.ssafy.kafkaorderlab.event.OrderCreatedEventContract.VERSION_2;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.EventHeaders;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
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
 * 주문 이벤트 발행을 담당한다.
 */
@Service
@Slf4j
public class OrderEventService {

	private final KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>> kafkaTemplate;
	private final String orderEventsTopic;

	public OrderEventService(
		KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>> kafkaTemplate,
		@Value("${app.kafka.topics.order-events}") String orderEventsTopic
	) {
		this.kafkaTemplate = kafkaTemplate;
		this.orderEventsTopic = orderEventsTopic;
	}

	/**
	 * 주문 생성 요청을 계약형 Kafka 이벤트로 발행한다.
	 * record key로 {@code orderId}를 사용하므로 같은 주문의 이벤트는 항상 같은 partition으로 간다.
	 *
	 * @param request 검증을 통과한 주문 요청
	 * @throws IllegalStateException Kafka 이벤트 발행 준비에 실패한 경우
	 */
	public void publishOrderCreated(CreateOrderRequest request) {
		EventEnvelope<OrderCreatedPayload> event = createEvent(request);
		ProducerRecord<String, EventEnvelope<OrderCreatedPayload>> record = new ProducerRecord<>(
			orderEventsTopic, request.orderId(), event);
		EventHeaders.addContractHeaders(record.headers(), event.eventType(), event.eventVersion());
		CompletableFuture<SendResult<String, EventEnvelope<OrderCreatedPayload>>> future = kafkaTemplate.send(record);
		future.whenComplete((result, error) -> {
			if (error != null) {
				log.error("order event publish failed: topic={}, key={}", orderEventsTopic, request.orderId(), error);
				return;
			}
			RecordMetadata metadata = result.getRecordMetadata();
			log.info("order event published: eventId={}, key={}, topic={}, partition={}, offset={}", event.eventId(),
				request.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
		});
	}

	private EventEnvelope<OrderCreatedPayload> createEvent(CreateOrderRequest request) {
		int eventVersion = request.couponCode() == null ? VERSION_1 : VERSION_2;
		return new EventEnvelope<>(UUID.randomUUID(), TYPE, eventVersion, Instant.now(),
			new OrderCreatedPayload(request.orderId(), request.amount(), request.couponCode()));
	}
}
