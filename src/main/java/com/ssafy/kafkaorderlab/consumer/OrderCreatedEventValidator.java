package com.ssafy.kafkaorderlab.consumer;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import org.springframework.stereotype.Component;

/**
 * 소비한 주문 생성 이벤트가 지원하는 계약인지 검증한다.
 */
@Component
class OrderCreatedEventValidator {

	private static final String ORDER_CREATED = "OrderCreated";

	/**
	 * 이벤트 종류, 버전, 필수 payload를 검증한다.
	 *
	 * @param event 역직렬화한 주문 생성 이벤트
	 * @throws InvalidOrderCreatedEventException 계약을 충족하지 않은 경우
	 */
	void validate(EventEnvelope<OrderCreatedPayload> event) {
		if (event.eventId() == null) {
			throw new InvalidOrderCreatedEventException("eventId is required");
		}
		if (event.occurredAt() == null) {
			throw new InvalidOrderCreatedEventException("occurredAt is required");
		}
		if (!ORDER_CREATED.equals(event.eventType())) {
			throw new InvalidOrderCreatedEventException("eventType must be OrderCreated");
		}
		if (event.eventVersion() != 1 && event.eventVersion() != 2) {
			throw new InvalidOrderCreatedEventException("eventVersion must be 1 or 2");
		}
		if (event.payload() == null || event.payload().orderId() == null || event.payload().orderId().isBlank()) {
			throw new InvalidOrderCreatedEventException("orderId is required");
		}
		if (event.payload().amount() <= 0) {
			throw new InvalidOrderCreatedEventException("amount must be positive");
		}
	}
}
