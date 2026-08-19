package com.ssafy.kafkaorderlab;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@AutoConfigureMockMvc
@DisplayName("주문 API의 Kafka 이벤트 발행")
class KafkaOrderLabApplicationTests {

	private static final String ORDER_ID = "1001";
	private static final long ORDER_AMOUNT = 15000L;
	private static final String ORDER_REQUEST_JSON = """
		{"orderId":"1001","amount":15000}
		""";
	private static final String ORDER_EVENT_JSON =
		"{\"orderId\":\"1001\",\"amount\":15000,\"type\":\"OrderCreated\"}";

	@Autowired
	private MockMvc mockMvc;

	@Value("${app.kafka.topics.order-events}")
	private String orderEventsTopic;

	@MockitoBean
	private KafkaTemplate<String, String> kafkaTemplate;

	@Test
	@DisplayName("주문 생성 요청을 받으면 OrderCreated 이벤트를 발행한다")
	void createsAnOrderAndPublishesOrderCreated() throws Exception {
		when(kafkaTemplate.send(eq(orderEventsTopic), eq(ORDER_ID), anyString()))
			.thenReturn(new CompletableFuture<>());

		mockMvc.perform(post("/orders")
					.contentType("application/json")
					.content(ORDER_REQUEST_JSON))
			.andExpect(status().isAccepted());

		verify(kafkaTemplate).send(eq(orderEventsTopic), eq(ORDER_ID), eq(ORDER_EVENT_JSON));
	}

}
