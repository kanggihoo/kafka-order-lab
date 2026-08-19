package com.ssafy.kafkaorderlab.controller;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.service.OrderEventService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.validation.annotation.Validated;

/**
 * 주문 요청을 받아 첫 Kafka 이벤트를 발행한다.
 */
@RestController
@Validated
class OrdersController {

	private final OrderEventService orderEventService;

	OrdersController(OrderEventService orderEventService) {
		this.orderEventService = orderEventService;
	}

	/**
	 * 주문 ID를 key로 사용해 {@code OrderCreated} 레코드를 발행한다.
	 *
	 * @param request 검증을 통과한 주문 ID와 금액
	 * @throws IllegalStateException 주문 이벤트를 JSON으로 변환하지 못한 경우
	 */
	@PostMapping("/orders")
	@ResponseStatus(HttpStatus.ACCEPTED)
	void create(@Validated @RequestBody CreateOrderRequest request) {
		orderEventService.publishOrderCreated(request);
	}

}
