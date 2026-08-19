package com.ssafy.kafkaorderlab.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

/**
 * 주문 생성 엔드포인트가 받는 입력값이다.
 *
 * @param orderId Kafka key로 사용할 UUID 형식 또는 증가형 주문 식별자
 * @param amount 양수 주문 금액
 */
public record CreateOrderRequest(@NotBlank @Pattern(regexp = "[0-9A-Za-z-]+") String orderId, @Positive long amount) {
}
