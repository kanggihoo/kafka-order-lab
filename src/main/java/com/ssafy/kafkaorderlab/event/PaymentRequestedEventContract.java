package com.ssafy.kafkaorderlab.event;

/**
 * 결제 요청 이벤트의 종류와 지원 버전을 정의한다.
 */
public final class PaymentRequestedEventContract {

	public static final String TYPE = "PaymentRequested";
	public static final int VERSION_1 = 1;

	private PaymentRequestedEventContract() {
	}
}
