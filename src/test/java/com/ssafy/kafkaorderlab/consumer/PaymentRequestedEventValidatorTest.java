package com.ssafy.kafkaorderlab.consumer;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("PaymentRequested 이벤트 계약 검증기")
class PaymentRequestedEventValidatorTest {

	private final PaymentRequestedEventValidator validator = new PaymentRequestedEventValidator();

	@Test
	@DisplayName("v1 이벤트를 허용한다")
	void acceptsSupportedVersion() {
		assertThatCode(() -> validator.validate(event(1, "1001", 15000L))).doesNotThrowAnyException();
	}

	@Test
	@DisplayName("지원하지 않는 버전은 계약 위반으로 처리한다")
	void rejectsUnsupportedVersion() {
		assertThatThrownBy(() -> validator.validate(event(2, "1001", 15000L)))
			.isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("eventVersion");
	}

	@Test
	@DisplayName("다른 종류의 이벤트는 계약 위반으로 처리한다")
	void rejectsOtherEventType() {
		assertThatThrownBy(() -> validator.validate(new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", 1,
			Instant.parse("2026-08-20T00:00:00Z"), new PaymentRequestedPayload("1001", 15000L))))
			.isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("eventType");
	}

	@Test
	@DisplayName("orderId가 없거나 금액이 0 이하면 계약 위반으로 처리한다")
	void rejectsInvalidPayload() {
		assertThatThrownBy(() -> validator.validate(event(1, null, 15000L)))
			.isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("orderId");
		assertThatThrownBy(() -> validator.validate(event(1, "1001", 0L)))
			.isInstanceOf(InvalidEventException.class)
			.hasMessageContaining("amount");
	}

	private EventEnvelope<PaymentRequestedPayload> event(int version, String orderId, long amount) {
		return new EventEnvelope<>(UUID.randomUUID(), "PaymentRequested", version,
			Instant.parse("2026-08-20T00:00:00Z"), new PaymentRequestedPayload(orderId, amount));
	}
}
