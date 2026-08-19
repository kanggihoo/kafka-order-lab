package com.ssafy.kafkaorderlab.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import java.util.concurrent.CompletableFuture;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

@ExtendWith(MockitoExtension.class)
@DisplayName("주문 이벤트 서비스")
class OrderEventServiceTest {

	private static final String ORDER_EVENTS_TOPIC = "order-events";
	private static final String ORDER_ID = "1001";
	private static final long ORDER_AMOUNT = 15000L;
	@Mock
	private KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>> kafkaTemplate;

	private OrderEventService service;

	@BeforeEach
	void setUp() {
		service = new OrderEventService(kafkaTemplate, ORDER_EVENTS_TOPIC);
	}

	@Test
	@DisplayName("couponCode가 없으면 OrderCreated v1 envelope를 발행한다")
	void publishesOrderCreatedV1Event() {
		when(kafkaTemplate.send(any(ProducerRecord.class)))
			.thenReturn(new CompletableFuture<>());

		service.publishOrderCreated(new CreateOrderRequest(ORDER_ID, ORDER_AMOUNT));

		org.mockito.ArgumentCaptor<ProducerRecord<String, EventEnvelope<OrderCreatedPayload>>> recordCaptor =
			org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
		verify(kafkaTemplate).send(recordCaptor.capture());
		EventEnvelope<OrderCreatedPayload> event = recordCaptor.getValue().value();

		assertThat(event.eventId()).isNotNull();
		assertThat(event.eventType()).isEqualTo("OrderCreated");
		assertThat(event.eventVersion()).isEqualTo(1);
		assertThat(event.occurredAt()).isNotNull();
		assertThat(event.payload()).isEqualTo(new OrderCreatedPayload(ORDER_ID, ORDER_AMOUNT, null));
	}

	@Test
	@DisplayName("couponCode가 있으면 OrderCreated v2 envelope를 발행한다")
	void publishesOrderCreatedV2Event() {
		when(kafkaTemplate.send(any(ProducerRecord.class)))
			.thenReturn(new CompletableFuture<>());

		service.publishOrderCreated(new CreateOrderRequest(ORDER_ID, ORDER_AMOUNT, "WELCOME"));

		org.mockito.ArgumentCaptor<ProducerRecord<String, EventEnvelope<OrderCreatedPayload>>> recordCaptor =
			org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
		verify(kafkaTemplate).send(recordCaptor.capture());
		EventEnvelope<OrderCreatedPayload> event = recordCaptor.getValue().value();

		assertThat(event.eventVersion()).isEqualTo(2);
		assertThat(event.payload()).isEqualTo(new OrderCreatedPayload(ORDER_ID, ORDER_AMOUNT, "WELCOME"));
	}

	@Test
	@DisplayName("발행 record header에 이벤트 종류와 버전을 넣는다")
	void addsEventContractHeaders() {
		when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

		service.publishOrderCreated(new CreateOrderRequest(ORDER_ID, ORDER_AMOUNT, "WELCOME"));

		org.mockito.ArgumentCaptor<ProducerRecord<String, EventEnvelope<OrderCreatedPayload>>> recordCaptor =
			org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
		verify(kafkaTemplate).send(recordCaptor.capture());
		ProducerRecord<String, EventEnvelope<OrderCreatedPayload>> record = recordCaptor.getValue();

		assertThat(new String(record.headers().lastHeader("eventType").value(), StandardCharsets.UTF_8)).isEqualTo("OrderCreated");
		assertThat(new String(record.headers().lastHeader("eventVersion").value(), StandardCharsets.UTF_8)).isEqualTo("2");
	}
}
