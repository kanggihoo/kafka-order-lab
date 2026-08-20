package com.ssafy.kafkaorderlab.event;

/**
 * 주문 생성 이벤트의 종류와 지원 버전을 정의한다.
 */
public final class OrderCreatedEventContract {

	public static final String TYPE = "OrderCreated";
	public static final int VERSION_1 = 1;
	public static final int VERSION_2 = 2;

	private OrderCreatedEventContract() {
	}
}
