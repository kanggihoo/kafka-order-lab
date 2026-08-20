package com.ssafy.kafkaorderlab.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
/**
 * 주문 생성 이벤트의 payload를 표현한다.
 *
 * @param orderId 주문 식별자이자 Kafka record key
 * @param amount 주문 금액
 * @param couponCode v2에서 추가된 선택 쿠폰 코드
 */
public record OrderCreatedPayload(
	String orderId,
	long amount,
	@JsonInclude(Include.NON_NULL) String couponCode
) {
}
