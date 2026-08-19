package com.ssafy.kafkaorderlab.consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("OrderCreated 이벤트 계약 검증기")
class OrderCreatedEventValidatorTest {

	private final OrderCreatedEventValidator validator = new OrderCreatedEventValidator();

	@Test
	@DisplayName("v1과 v2 이벤트를 허용한다")
	void acceptsSupportedVersions() {
		assertThatCode(() -> validator.validate(event(1, "1001", 15000L, null))).doesNotThrowAnyException();
		assertThatCode(() -> validator.validate(event(2, "1001", 15000L, "WELCOME"))).doesNotThrowAnyException();
	}

	@Test
	@DisplayName("orderId가 없으면 계약 위반으로 처리한다")
	void rejectsMissingOrderId() {
		assertThatThrownBy(() -> validator.validate(event(1, null, 15000L, null)))
			.isInstanceOf(InvalidOrderCreatedEventException.class)
			.hasMessageContaining("orderId");
	}

	@Test
	@DisplayName("지원하지 않는 버전은 계약 위반으로 처리한다")
	void rejectsUnsupportedVersion() {
		assertThatThrownBy(() -> validator.validate(event(3, "1001", 15000L, null)))
			.isInstanceOf(InvalidOrderCreatedEventException.class)
			.hasMessageContaining("eventVersion");
	}

	@Test
	@DisplayName("eventId 또는 occurredAt이 없으면 계약 위반으로 처리한다")
	void rejectsMissingEnvelopeFields() {
		assertThatThrownBy(() -> validator.validate(new EventEnvelope<>(null, "OrderCreated", 1,
			Instant.parse("2026-08-20T00:00:00Z"), new OrderCreatedPayload("1001", 15000L, null))))
			.isInstanceOf(InvalidOrderCreatedEventException.class)
			.hasMessageContaining("eventId");
		assertThatThrownBy(() -> validator.validate(new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", 1,
			null, new OrderCreatedPayload("1001", 15000L, null))))
			.isInstanceOf(InvalidOrderCreatedEventException.class)
			.hasMessageContaining("occurredAt");
	}

	private EventEnvelope<OrderCreatedPayload> event(int version, String orderId, long amount, String couponCode) {
		return new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", version, Instant.parse("2026-08-20T00:00:00Z"),
			new OrderCreatedPayload(orderId, amount, couponCode));
	}
}
