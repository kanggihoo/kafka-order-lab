package com.ssafy.kafkaorderlab.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("Kafka JSON 역직렬화 설정")
class KafkaJsonConfigurationTest {

	private final KafkaJsonConfiguration configuration = new KafkaJsonConfiguration();

	@Test
	@DisplayName("OrderCreated v2 JSON을 계약형 envelope로 역직렬화한다")
	void deserializesOrderCreatedV2() {
		JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> deserializer = configuration.orderCreatedValueDeserializer();

		EventEnvelope<OrderCreatedPayload> event = deserializer.deserialize("order-events", v2Json().getBytes(StandardCharsets.UTF_8));

		assertThat(event.eventType()).isEqualTo("OrderCreated");
		assertThat(event.eventVersion()).isEqualTo(2);
		assertThat(event.payload()).isEqualTo(new OrderCreatedPayload("1001", 15000L, "WELCOME"));
	}

	@Test
	@DisplayName("amount가 문자열이면 역직렬화에 실패한다")
	void rejectsStringAmount() {
		JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> deserializer = configuration.orderCreatedValueDeserializer();
		String invalidJson = v2Json().replace("\"amount\":15000", "\"amount\":\"15000\"");

		assertThatThrownBy(() -> deserializer.deserialize("order-events", invalidJson.getBytes(StandardCharsets.UTF_8)))
			.isInstanceOf(Exception.class);
	}

	@Test
	@DisplayName("v1 JSON은 couponCode 필드를 포함하지 않는다")
	void omitsCouponCodeFromV1Json() throws Exception {
		JacksonJsonSerializer<EventEnvelope<OrderCreatedPayload>> serializer = new JacksonJsonSerializer<>();
		EventEnvelope<OrderCreatedPayload> event = new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", 1,
			Instant.parse("2026-08-20T00:00:00Z"), new OrderCreatedPayload("1001", 15000L, null));

		byte[] json = serializer.serialize("order-events", event);

		assertThat(new ObjectMapper().readTree(json).path("payload").has("couponCode")).isFalse();
	}

	@Test
	@DisplayName("v1 consumer는 v2의 선택 couponCode를 무시하고 읽는다")
	void legacyConsumerIgnoresOptionalCouponCode() {
		JsonMapper legacyMapper = JsonMapper.builder().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();
		JacksonJsonDeserializer<EventEnvelope<LegacyOrderCreatedPayload>> deserializer =
			new JacksonJsonDeserializer<>(new TypeReference<EventEnvelope<LegacyOrderCreatedPayload>>() { }, legacyMapper)
				.ignoreTypeHeaders();

		EventEnvelope<LegacyOrderCreatedPayload> event = deserializer.deserialize(
			"order-events", v2Json().getBytes(StandardCharsets.UTF_8));

		assertThat(event.payload()).isEqualTo(new LegacyOrderCreatedPayload("1001", 15000L));
	}

	private String v2Json() {
		return """
			{"eventId":"7d1d1cb2-9b4d-4c67-b512-1e1d9f4c5290","eventType":"OrderCreated","eventVersion":2,
			"occurredAt":"2026-08-20T00:00:00Z","payload":{"orderId":"1001","amount":15000,"couponCode":"WELCOME"}}
			""";
	}

	private record LegacyOrderCreatedPayload(String orderId, long amount) {
	}
}
