package com.ssafy.kafkaorderlab.controller;

import com.ssafy.kafkaorderlab.dto.ErrorResponse;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import jakarta.servlet.http.HttpServletRequest;

/**
 * HTTP 요청 검증 실패를 공통 오류 응답으로 변환한다.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

	/**
	 * Bean Validation 실패를 400 공통 오류 응답으로 반환한다.
	 *
	 * @param exception 검증 실패 예외
	 * @param request 실패한 HTTP 요청
	 * @return 필드 오류를 포함한 400 응답
	 */
	@ExceptionHandler(MethodArgumentNotValidException.class)
	public ResponseEntity<ErrorResponse> handleValidation(
		MethodArgumentNotValidException exception,
		HttpServletRequest request
	) {
		Map<String, String> errors = new LinkedHashMap<>();
		for (FieldError fieldError : exception.getBindingResult().getFieldErrors()) {
			errors.putIfAbsent(fieldError.getField(), fieldError.getDefaultMessage());
		}
		ErrorResponse body = new ErrorResponse(false, HttpStatus.BAD_REQUEST.value(), "입력값이 올바르지 않습니다.",
			"VALIDATION_ERROR", errors, Instant.now(), request.getRequestURI());
		return ResponseEntity.badRequest().body(body);
	}
}
