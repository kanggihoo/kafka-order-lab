package com.ssafy.kafkaorderlab.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
@DisplayName("결제 이벤트 서비스")
class PaymentEventServiceTest {

	private static final String PAYMENT_EVENTS_TOPIC = "payment-events";
	private static final String ORDER_ID = "1001";
	private static final long ORDER_AMOUNT = 15000L;

	@Mock
	private KafkaTemplate<String, EventEnvelope<PaymentRequestedPayload>> kafkaTemplate;

	private PaymentEventService service;

	@BeforeEach
	void setUp() {
		service = new PaymentEventService(kafkaTemplate, PAYMENT_EVENTS_TOPIC);
	}

	@Test
	@DisplayName("주문과 같은 orderId를 key로 PaymentRequested v1 envelope를 발행한다")
	void publishesPaymentRequestedWithOrderIdKey() {
		when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

		service.publishPaymentRequested(ORDER_ID, ORDER_AMOUNT);

		ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>> record = capturedRecord();
		assertThat(record.topic()).isEqualTo(PAYMENT_EVENTS_TOPIC);
		assertThat(record.key()).isEqualTo(ORDER_ID);
		assertThat(record.value().eventId()).isNotNull();
		assertThat(record.value().eventType()).isEqualTo("PaymentRequested");
		assertThat(record.value().eventVersion()).isEqualTo(1);
		assertThat(record.value().occurredAt()).isNotNull();
		assertThat(record.value().payload()).isEqualTo(new PaymentRequestedPayload(ORDER_ID, ORDER_AMOUNT));
	}

	@Test
	@DisplayName("발행 record header에 이벤트 종류와 버전을 넣는다")
	void addsEventContractHeaders() {
		when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

		service.publishPaymentRequested(ORDER_ID, ORDER_AMOUNT);

		ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>> record = capturedRecord();
		assertThat(headerValue(record, "eventType")).isEqualTo("PaymentRequested");
		assertThat(headerValue(record, "eventVersion")).isEqualTo("1");
	}

	private ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>> capturedRecord() {
		ArgumentCaptor<ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>>> recordCaptor =
			ArgumentCaptor.forClass(ProducerRecord.class);
		verify(kafkaTemplate).send(recordCaptor.capture());
		return recordCaptor.getValue();
	}

	private String headerValue(ProducerRecord<String, EventEnvelope<PaymentRequestedPayload>> record, String name) {
		return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
	}
}
