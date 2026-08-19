package com.ssafy.kafkaorderlab.consumer;

/**
 * 주문 생성 이벤트가 소비 계약을 충족하지 않을 때 발생한다.
 */
class InvalidOrderCreatedEventException extends RuntimeException {

	InvalidOrderCreatedEventException(String message) {
		super(message);
	}
}
