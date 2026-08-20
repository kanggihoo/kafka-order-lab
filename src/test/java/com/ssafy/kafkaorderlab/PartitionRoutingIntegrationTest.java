package com.ssafy.kafkaorderlab;

import static org.assertj.core.api.Assertions.assertThat;

import com.ssafy.kafkaorderlab.dto.CreateOrderRequest;
import com.ssafy.kafkaorderlab.service.OrderEventService;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

@EmbeddedKafka(
	partitions = PartitionRoutingIntegrationTest.PARTITION_COUNT,
	topics = {PartitionRoutingIntegrationTest.ORDER_EVENTS_TOPIC, PartitionRoutingIntegrationTest.PAYMENT_EVENTS_TOPIC}
)
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@DisplayName("orderId key의 partition 라우팅과 결제 흐름 통합 테스트")
class PartitionRoutingIntegrationTest {

	static final int PARTITION_COUNT = 3;
	static final String ORDER_EVENTS_TOPIC = "order-events";
	static final String PAYMENT_EVENTS_TOPIC = "payment-events";

	private static final String REPEATED_ORDER_ID = "1001";
	private static final int REPEAT_COUNT = 3;
	private static final List<String> SPREAD_ORDER_IDS =
		List.of("1002", "1003", "1004", "1005", "1006", "1007", "1008", "1009", "1010");
	private static final long ORDER_AMOUNT = 15000L;
	private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);
	private static final Duration COLLECT_TIMEOUT = Duration.ofSeconds(30);

	@Autowired
	private OrderEventService orderEventService;

	@Autowired
	private EmbeddedKafkaBroker broker;

	@Test
	@DisplayName("같은 orderId는 항상 같은 partition으로 가고 다른 orderId는 여러 partition에 분산된다")
	void routesSameOrderIdToSamePartition() {
		publishOrders();
		int expectedRecordCount = REPEAT_COUNT + SPREAD_ORDER_IDS.size();

		List<ConsumerRecord<String, String>> orderRecords = collectRecords(ORDER_EVENTS_TOPIC, expectedRecordCount);
		List<ConsumerRecord<String, String>> paymentRecords = collectRecords(PAYMENT_EVENTS_TOPIC, expectedRecordCount);

		assertThat(partitionsOf(orderRecords, REPEATED_ORDER_ID)).hasSize(1);
		assertThat(partitionsOf(paymentRecords, REPEATED_ORDER_ID)).hasSize(1);
		assertThat(partitionsOf(orderRecords, REPEATED_ORDER_ID))
			.isEqualTo(partitionsOf(paymentRecords, REPEATED_ORDER_ID));
		assertThat(distinctPartitions(orderRecords)).hasSizeGreaterThan(1);
	}

	@Test
	@DisplayName("payment-service는 주문 생성 이벤트마다 같은 key로 PaymentRequested를 발행한다")
	void publishesPaymentRequestedForEachOrderCreated() {
		publishOrders();
		int expectedRecordCount = REPEAT_COUNT + SPREAD_ORDER_IDS.size();

		List<ConsumerRecord<String, String>> paymentRecords = collectRecords(PAYMENT_EVENTS_TOPIC, expectedRecordCount);

		assertThat(paymentRecords).hasSizeGreaterThanOrEqualTo(expectedRecordCount);
		assertThat(paymentRecords).allSatisfy(record -> {
			assertThat(record.value()).contains("\"eventType\":\"PaymentRequested\"");
			assertThat(record.value()).contains("\"orderId\":\"" + record.key() + "\"");
		});
	}

	private void publishOrders() {
		for (int attempt = 0; attempt < REPEAT_COUNT; attempt++) {
			orderEventService.publishOrderCreated(new CreateOrderRequest(REPEATED_ORDER_ID, ORDER_AMOUNT));
		}
		for (String orderId : SPREAD_ORDER_IDS) {
			orderEventService.publishOrderCreated(new CreateOrderRequest(orderId, ORDER_AMOUNT));
		}
	}

	private List<ConsumerRecord<String, String>> collectRecords(String topic, int expectedCount) {
		List<ConsumerRecord<String, String>> collected = new ArrayList<>();
		Instant deadline = Instant.now().plus(COLLECT_TIMEOUT);
		try (Consumer<String, String> consumer = createConsumer(topic)) {
			consumer.subscribe(List.of(topic));
			while (collected.size() < expectedCount && Instant.now().isBefore(deadline)) {
				ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
				records.records(topic).forEach(collected::add);
			}
		}
		return collected;
	}

	private Consumer<String, String> createConsumer(String topic) {
		Map<String, Object> properties = Map.of(
			ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
			ConsumerConfig.GROUP_ID_CONFIG, "partition-routing-test-" + topic,
			ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
		);
		return new DefaultKafkaConsumerFactory<String, String>(properties, new StringDeserializer(),
			new StringDeserializer()).createConsumer();
	}

	private Set<Integer> partitionsOf(List<ConsumerRecord<String, String>> records, String key) {
		return records.stream()
			.filter(record -> key.equals(record.key()))
			.map(ConsumerRecord::partition)
			.collect(Collectors.toSet());
	}

	private Set<Integer> distinctPartitions(List<ConsumerRecord<String, String>> records) {
		return records.stream().map(ConsumerRecord::partition).collect(Collectors.toSet());
	}
}
