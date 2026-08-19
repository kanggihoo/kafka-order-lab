package com.ssafy.kafkaorderlab.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * 주문 생성 엔드포인트가 받는 입력값이다.
 *
 * @param orderId Kafka key로 사용할 UUID 형식 또는 증가형 주문 식별자
 * @param amount 양수 주문 금액
 * @param couponCode 선택 쿠폰 코드이며 제공 시 공백일 수 없음
 */
public record CreateOrderRequest(
	@NotBlank @Pattern(regexp = "[0-9A-Za-z-]+") String orderId,
	@Positive long amount,
	@Pattern(regexp = ".*\\S.*") String couponCode
) {

	/**
	 * 쿠폰 코드가 없는 v1 요청을 생성한다.
	 *
	 * @param orderId 주문 식별자
	 * @param amount 주문 금액
	 */
	public CreateOrderRequest(String orderId, long amount) {
		this(orderId, amount, null);
	}
}
