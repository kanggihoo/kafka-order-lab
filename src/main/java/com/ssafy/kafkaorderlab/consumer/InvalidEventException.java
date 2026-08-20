package com.ssafy.kafkaorderlab.consumer;

/**
 * 소비한 Kafka 이벤트가 소비 계약을 충족하지 않을 때 발생한다.
 */
class InvalidEventException extends RuntimeException {

	InvalidEventException(String message) {
		super(message);
	}
}
