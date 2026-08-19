package com.ssafy.kafkaorderlab;

import static org.assertj.core.api.Assertions.assertThat;

import com.ssafy.kafkaorderlab.config.KafkaJsonConfiguration;
import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.beans.factory.annotation.Autowired;

@EmbeddedKafka(partitions = 1, topics = OrderEventJsonIntegrationTest.ORDER_EVENTS_TOPIC)
@SpringBootTest(properties = {
	"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
	"spring.kafka.listener.auto-startup=false"
})
@DisplayName("Kafka 주문 이벤트 JSON 통합 테스트")
class OrderEventJsonIntegrationTest {

	static final String ORDER_EVENTS_TOPIC = "order-events";

	@Autowired
	private EmbeddedKafkaBroker broker;

	@Test
	@DisplayName("v1과 v2 JSON을 실제 Kafka record로 역직렬화한다")
	void deserializesV1AndV2RecordsFromKafka() throws Exception {
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
			producer.send(new ProducerRecord<>(ORDER_EVENTS_TOPIC, "1001", v1Json())).get();
			producer.send(new ProducerRecord<>(ORDER_EVENTS_TOPIC, "1002", v2Json())).get();
		}

		try (Consumer<String, EventEnvelope<OrderCreatedPayload>> consumer = createConsumer()) {
			consumer.subscribe(List.of(ORDER_EVENTS_TOPIC));
			ConsumerRecords<String, EventEnvelope<OrderCreatedPayload>> records = consumer.poll(Duration.ofSeconds(10));
			Iterator<ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>>> received =
				records.records(ORDER_EVENTS_TOPIC).iterator();
			ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>> first = received.next();
			ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>> second = received.next();

			assertThat(first.value().eventVersion()).isEqualTo(1);
			assertThat(first.value().payload().couponCode()).isNull();
			assertThat(second.value().eventVersion()).isEqualTo(2);
			assertThat(second.value().payload().couponCode()).isEqualTo("WELCOME");
		}
	}

	private Consumer<String, EventEnvelope<OrderCreatedPayload>> createConsumer() {
		JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> deserializer =
			new KafkaJsonConfiguration().orderCreatedValueDeserializer();
		return new DefaultKafkaConsumerFactory<String, EventEnvelope<OrderCreatedPayload>>(
			consumerProperties(), new StringDeserializer(), deserializer).createConsumer();
	}

	private Map<String, Object> producerProperties() {
		return Map.of(
			ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
			ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
			ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
		);
	}

	private Map<String, Object> consumerProperties() {
		return Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
			ConsumerConfig.GROUP_ID_CONFIG, "order-event-json-integration-test",
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
		);
	}

	private String v1Json() {
		return """
			{"eventId":"7d1d1cb2-9b4d-4c67-b512-1e1d9f4c5290","eventType":"OrderCreated","eventVersion":1,
			"occurredAt":"2026-08-20T00:00:00Z","payload":{"orderId":"1001","amount":15000}}
			""";
	}

	private String v2Json() {
		return """
			{"eventId":"d0fbfe1a-79db-4a13-8c94-4d3d9944b661","eventType":"OrderCreated","eventVersion":2,
			"occurredAt":"2026-08-20T00:00:00Z","payload":{"orderId":"1002","amount":15000,"couponCode":"WELCOME"}}
			""";
	}
}
