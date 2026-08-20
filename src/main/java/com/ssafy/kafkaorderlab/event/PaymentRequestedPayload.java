package com.ssafy.kafkaorderlab.event;

/**
 * 결제 요청 이벤트의 payload를 표현한다.
 *
 * @param orderId 결제를 요청한 주문 식별자이자 Kafka record key
 * @param amount 결제 요청 금액
 */
public record PaymentRequestedPayload(String orderId, long amount) {
}
