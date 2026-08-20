package com.ssafy.kafkaorderlab.consumer;

import static com.ssafy.kafkaorderlab.event.PaymentRequestedEventContract.TYPE;
import static com.ssafy.kafkaorderlab.event.PaymentRequestedEventContract.VERSION_1;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import org.springframework.stereotype.Component;

/**
 * 소비한 결제 요청 이벤트가 지원하는 계약인지 검증한다.
 */
@Component
class PaymentRequestedEventValidator {

	/**
	 * 이벤트 종류, 버전, 필수 payload를 검증한다.
	 *
	 * @param event 역직렬화한 결제 요청 이벤트
	 * @throws InvalidEventException 계약을 충족하지 않은 경우
	 */
	void validate(EventEnvelope<PaymentRequestedPayload> event) {
		if (event.eventId() == null) {
			throw new InvalidEventException("eventId is required");
		}
		if (event.occurredAt() == null) {
			throw new InvalidEventException("occurredAt is required");
		}
		if (!TYPE.equals(event.eventType())) {
			throw new InvalidEventException("eventType must be PaymentRequested");
		}
		if (event.eventVersion() != VERSION_1) {
			throw new InvalidEventException("eventVersion must be 1");
		}
		if (event.payload() == null || event.payload().orderId() == null || event.payload().orderId().isBlank()) {
			throw new InvalidEventException("orderId is required");
		}
		if (event.payload().amount() <= 0) {
			throw new InvalidEventException("amount must be positive");
		}
	}
}
