package com.ssafy.kafkaorderlab.event;

/**
 * Kafka에 발행할 주문 생성 이벤트의 구조를 정의한다.
 *
 * @param orderId 주문 식별자
 * @param amount 주문 금액
 * @param type 이벤트 종류
 */
public record OrderCreatedEvent(String orderId, long amount, String type) {
}
