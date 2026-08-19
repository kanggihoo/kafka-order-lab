package com.ssafy.kafkaorderlab;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
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
	@Autowired
	private MockMvc mockMvc;

	@Value("${app.kafka.topics.order-events}")
	private String orderEventsTopic;

	@MockitoBean
	private KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>> kafkaTemplate;

	@Test
	@DisplayName("주문 생성 요청을 받으면 OrderCreated 이벤트를 발행한다")
	void createsAnOrderAndPublishesOrderCreated() throws Exception {
		when(kafkaTemplate.send(any(ProducerRecord.class)))
			.thenReturn(new CompletableFuture<>());

		mockMvc.perform(post("/orders")
					.contentType("application/json")
					.content(ORDER_REQUEST_JSON))
			.andExpect(status().isAccepted());

		org.mockito.ArgumentCaptor<ProducerRecord<String, EventEnvelope<OrderCreatedPayload>>> recordCaptor =
			org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
		verify(kafkaTemplate).send(recordCaptor.capture());
		ProducerRecord<String, EventEnvelope<OrderCreatedPayload>> record = recordCaptor.getValue();
		assertThat(record.topic()).isEqualTo(orderEventsTopic);
		assertThat(record.key()).isEqualTo(ORDER_ID);
		assertThat(record.value().eventVersion()).isEqualTo(1);
		assertThat(record.value().payload()).isEqualTo(new OrderCreatedPayload(ORDER_ID, ORDER_AMOUNT, null));
	}

}
