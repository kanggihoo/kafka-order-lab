package com.ssafy.kafkaorderlab.service;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.event.OrderCreatedEvent;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

/**
 * 주문 이벤트 발행을 담당한다.
 */
@Service
@Slf4j
public class OrderEventService {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final ObjectMapper objectMapper;
	private final String orderEventsTopic;

	public OrderEventService(
		KafkaTemplate<String, String> kafkaTemplate,
		ObjectMapper objectMapper,
		@Value("${app.kafka.topics.order-events}") String orderEventsTopic
	) {
		this.kafkaTemplate = kafkaTemplate;
		this.objectMapper = objectMapper;
		this.orderEventsTopic = orderEventsTopic;
	}

	public void publishOrderCreated(CreateOrderRequest request) {
		String event = toJson(new OrderCreatedEvent(request.orderId(), request.amount(), "OrderCreated"));
		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(orderEventsTopic, request.orderId(), event);
		future.whenComplete((result, error) -> {
			if (error != null) {
				log.error("order event publish failed: key={}", request.orderId(), error);
				return;
			}
			RecordMetadata metadata = result.getRecordMetadata();
			log.info("order event published: key={}, partition={}, offset={}", request.orderId(), metadata.partition(), metadata.offset());
		});
	}

	private String toJson(OrderCreatedEvent event) {
		try {
			return objectMapper.writeValueAsString(event);
		} catch (JacksonException e) {
			throw new IllegalStateException("주문 이벤트 JSON 변환에 실패했습니다.", e);
		}
	}
}
