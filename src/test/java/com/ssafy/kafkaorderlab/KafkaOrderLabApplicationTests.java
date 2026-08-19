package com.ssafy.kafkaorderlab;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@AutoConfigureMockMvc
class KafkaOrderLabApplicationTests {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void createsAnOrderAndPublishesOrderCreated() throws Exception {
		when(kafkaTemplate.send(eq("order-events"), eq("1001"), anyString()))
			.thenReturn(new CompletableFuture<>());

		mockMvc.perform(post("/orders")
					.contentType("application/json")
					.content("{\"orderId\":\"1001\",\"amount\":15000}"))
			.andExpect(status().isAccepted());

		verify(kafkaTemplate).send(eq("order-events"), eq("1001"),
				eq("{\"orderId\":\"1001\",\"amount\":15000,\"type\":\"OrderCreated\"}"));
	}

}
