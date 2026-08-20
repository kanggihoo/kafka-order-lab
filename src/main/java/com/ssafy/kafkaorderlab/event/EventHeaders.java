package com.ssafy.kafkaorderlab.event;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Headers;

/**
 * 모든 Kafka 이벤트가 공통으로 사용하는 record header 계약을 정의한다.
 */
public final class EventHeaders {

	public static final String EVENT_TYPE = "eventType";
	public static final String EVENT_VERSION = "eventVersion";

	private EventHeaders() {
	}

	/**
	 * 발행할 record header에 이벤트 종류와 계약 버전을 추가한다.
	 *
	 * @param headers 발행할 record의 header
	 * @param eventType 이벤트 종류
	 * @param eventVersion 이벤트 계약 버전
	 */
	public static void addContractHeaders(Headers headers, String eventType, int eventVersion) {
		headers.add(EVENT_TYPE, eventType.getBytes(StandardCharsets.UTF_8));
		headers.add(EVENT_VERSION, String.valueOf(eventVersion).getBytes(StandardCharsets.UTF_8));
	}
}
