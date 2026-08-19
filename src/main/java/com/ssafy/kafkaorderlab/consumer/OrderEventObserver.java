package com.ssafy.kafkaorderlab.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 주문 이벤트를 소비하고 Kafka 메타데이터를 로그로 남긴다.
 */
@Component
@Slf4j
class OrderEventObserver {

	@KafkaListener(
		topics = "${app.kafka.topics.order-events}",
		groupId = "${app.kafka.consumer.group-id}"
	)
	void observe(ConsumerRecord<String, String> record) {
		log.info("order event observed: key={}, partition={}, offset={}, value={}",
			record.key(), record.partition(), record.offset(), record.value());
	}
}
