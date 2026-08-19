package com.ssafy.kafkaorderlab;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.validation.annotation.Validated;

/**
 * 주문 요청을 받아 첫 Kafka 이벤트를 발행한다.
 */
@RestController
@Validated
class OrdersController {

	private static final Logger log = LoggerFactory.getLogger(OrdersController.class);
	private final KafkaTemplate<String, String> kafkaTemplate;

	OrdersController(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	/**
	 * 주문 ID를 key로 사용해 {@code OrderCreated} 레코드를 발행한다.
	 *
	 * @param request 검증을 통과한 주문 ID와 금액
	 */
	@PostMapping("/orders")
	@ResponseStatus(HttpStatus.ACCEPTED)
	void create(@Validated @RequestBody CreateOrderRequest request) {
		String event = "{\"orderId\":\"%s\",\"amount\":%d,\"type\":\"OrderCreated\"}"
			.formatted(request.orderId(), request.amount());
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("order-events", request.orderId(), event);
		future.whenComplete((result, error) -> {
			if (error != null) {
				log.error("order event publish failed: key={}", request.orderId(), error);
				return;
			}
			var metadata = result.getRecordMetadata();
			log.info("order event published: key={}, partition={}, offset={}", request.orderId(), metadata.partition(), metadata.offset());
		});
	}

	/**
	 * 학습 과정에서 key, partition, offset을 확인할 수 있도록 Kafka 메타데이터를 로그로 남긴다.
	 *
	 * @param record 소비한 주문 이벤트
	 */
	@KafkaListener(topics = "order-events", groupId = "order-observer")
	void observe(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
		log.info("order event observed: key={}, partition={}, offset={}, value={}",
			record.key(), record.partition(), record.offset(), record.value());
	}

	/**
	 * 주문 생성 엔드포인트가 받는 입력값이다.
	 *
	 * @param orderId Kafka key로 사용할 UUID 형식 또는 증가형 주문 식별자
	 * @param amount 양수 주문 금액
	 */
	record CreateOrderRequest(@NotBlank @Pattern(regexp = "[0-9A-Za-z-]+") String orderId, @Positive long amount) {
	}
}
