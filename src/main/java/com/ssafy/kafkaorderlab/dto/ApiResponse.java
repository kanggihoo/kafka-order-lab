package com.ssafy.kafkaorderlab.dto;

import java.time.Instant;

/**
 * 성공 HTTP 응답의 공통 형식이다.
 *
 * @param success 성공 여부
 * @param status HTTP 상태 코드
 * @param message 응답 메시지
 * @param data 응답 데이터
 * @param timestamp 응답 생성 시각
 * @param <T> 응답 데이터 타입
 */
public record ApiResponse<T>(boolean success, int status, String message, T data, Instant timestamp) {

	/**
	 * 데이터가 없는 성공 응답을 생성한다.
	 *
	 * @param status HTTP 상태 코드
	 * @param message 응답 메시지
	 * @return 공통 성공 응답
	 */
	public static ApiResponse<Void> accepted(int status, String message) {
		return new ApiResponse<>(true, status, message, null, Instant.now());
	}
}
