package com.ssafy.kafkaorderlab.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.service.OrderEventService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(OrdersController.class)
@DisplayName("주문 Controller MVC 테스트")
class OrdersControllerWebMvcTest {

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private OrderEventService orderEventService;

	@Test
	@DisplayName("유효한 주문 요청은 202 응답을 반환하고 서비스에 전달한다")
	void acceptsValidOrderRequest() throws Exception {
		mockMvc.perform(post("/orders")
					.contentType("application/json")
					.content("""
						{"orderId":"1001","amount":15000}
						"""))
			.andExpect(status().isAccepted())
			.andExpect(jsonPath("$.success").value(true))
			.andExpect(jsonPath("$.status").value(202))
			.andExpect(jsonPath("$.data").doesNotExist());

		verify(orderEventService).publishOrderCreated(any(CreateOrderRequest.class));
	}

	@Test
	@DisplayName("유효하지 않은 주문 요청은 400 응답을 반환하고 서비스를 호출하지 않는다")
	void rejectsInvalidOrderRequest() throws Exception {
		mockMvc.perform(post("/orders")
					.contentType("application/json")
					.content("""
						{"orderId":"","amount":0}
						"""))
			.andExpect(status().isBadRequest())
			.andExpect(jsonPath("$.success").value(false))
			.andExpect(jsonPath("$.status").value(400))
			.andExpect(jsonPath("$.errorCode").value("VALIDATION_ERROR"))
			.andExpect(jsonPath("$.errors.orderId").exists())
			.andExpect(jsonPath("$.path").value("/orders"));

		verifyNoInteractions(orderEventService);
	}
}
