package com.ssafy.kafkaorderlab.event;

/**
 * 주문 생성 이벤트의 종류, 지원 버전, Kafka 헤더 계약을 정의한다.
 */
public final class OrderCreatedEventContract {

	public static final String TYPE = "OrderCreated";
	public static final int VERSION_1 = 1;
	public static final int VERSION_2 = 2;
	public static final String EVENT_TYPE_HEADER = "eventType";
	public static final String EVENT_VERSION_HEADER = "eventVersion";

	private OrderCreatedEventContract() {
	}
}
