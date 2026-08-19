package com.ssafy.kafkaorderlab.controller;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.dto.ApiResponse;
import com.ssafy.kafkaorderlab.service.OrderEventService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
	 * @return 주문 이벤트 접수 성공 응답
	 */
	@PostMapping("/orders")
	ResponseEntity<ApiResponse<Void>> create(@Valid @RequestBody CreateOrderRequest request) {
		orderEventService.publishOrderCreated(request);
		return ResponseEntity.status(HttpStatus.ACCEPTED)
			.body(ApiResponse.accepted(HttpStatus.ACCEPTED.value(), "주문 생성 이벤트를 접수했습니다."));
	}

}
