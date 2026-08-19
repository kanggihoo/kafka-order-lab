package com.ssafy.kafkaorderlab.service;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
@DisplayName("주문 이벤트 서비스")
class OrderEventServiceTest {

	private static final String ORDER_EVENTS_TOPIC = "order-events";
	private static final String ORDER_ID = "1001";
	private static final long ORDER_AMOUNT = 15000L;
	private static final String ORDER_EVENT_JSON =
		"{\"orderId\":\"1001\",\"amount\":15000,\"type\":\"OrderCreated\"}";

	@Mock
	private KafkaTemplate<String, String> kafkaTemplate;

	@Test
	@DisplayName("주문 생성 이벤트를 JSON으로 변환해 설정된 topic으로 발행한다")
	void publishesOrderCreatedEventToConfiguredTopic() {
		when(kafkaTemplate.send(eq(ORDER_EVENTS_TOPIC), eq(ORDER_ID), eq(ORDER_EVENT_JSON)))
			.thenReturn(new CompletableFuture<>());
		OrderEventService service = new OrderEventService(kafkaTemplate, new ObjectMapper(), ORDER_EVENTS_TOPIC);

		service.publishOrderCreated(new CreateOrderRequest(ORDER_ID, ORDER_AMOUNT));

		verify(kafkaTemplate).send(ORDER_EVENTS_TOPIC, ORDER_ID, ORDER_EVENT_JSON);
	}
}
