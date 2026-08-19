package com.ssafy.kafkaorderlab.event;

import java.time.Instant;
import java.util.UUID;

/**
 * Kafka 이벤트의 공통 계약을 표현한다.
 *
 * @param eventId 전달 중복을 식별하는 이벤트 식별자
 * @param eventType 이벤트 종류
 * @param eventVersion 이벤트 계약 버전
 * @param occurredAt 이벤트 발생 시각
 * @param payload 이벤트별 데이터
 * @param <T> 이벤트별 payload 타입
 */
public record EventEnvelope<T>(UUID eventId, String eventType, int eventVersion, Instant occurredAt, T payload) {
}
