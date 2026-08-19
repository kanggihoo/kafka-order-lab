package com.ssafy.kafkaorderlab.dto;

import java.time.Instant;
import java.util.Map;

/**
 * 실패 HTTP 응답의 공통 형식이다.
 *
 * @param success 성공 여부
 * @param status HTTP 상태 코드
 * @param message 사용자에게 보여줄 오류 메시지
 * @param errorCode 애플리케이션 오류 코드
 * @param errors 필드별 오류 메시지
 * @param timestamp 응답 생성 시각
 * @param path 요청 경로
 */
public record ErrorResponse(
	boolean success,
	int status,
	String message,
	String errorCode,
	Map<String, String> errors,
	Instant timestamp,
	String path
) {
}
