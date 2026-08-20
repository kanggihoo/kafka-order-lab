package com.ssafy.kafkaorderlab.config;

import com.ssafy.kafkaorderlab.event.EventEnvelope;
import com.ssafy.kafkaorderlab.event.OrderCreatedPayload;
import com.ssafy.kafkaorderlab.event.PaymentRequestedPayload;
import java.util.Map;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.cfg.CoercionAction;
import tools.jackson.databind.cfg.CoercionInputShape;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.LogicalType;

/**
 * 주문·결제 이벤트의 JSON 역직렬화와 Kafka listener container를 설정한다.
 */
@Configuration
public class KafkaJsonConfiguration {

	/**
	 * OrderCreated envelope만 역직렬화하는 JSON deserializer를 생성한다.
	 *
	 * @return 신뢰 패키지와 목표 타입을 제한한 deserializer
	 */
	public JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> orderCreatedValueDeserializer() {
		return new JacksonJsonDeserializer<>(new TypeReference<EventEnvelope<OrderCreatedPayload>>() { }, strictJsonMapper())
			.trustedPackages("com.ssafy.kafkaorderlab.event")
			.ignoreTypeHeaders();
	}

	/**
	 * PaymentRequested envelope만 역직렬화하는 JSON deserializer를 생성한다.
	 *
	 * @return 신뢰 패키지와 목표 타입을 제한한 deserializer
	 */
	public JacksonJsonDeserializer<EventEnvelope<PaymentRequestedPayload>> paymentRequestedValueDeserializer() {
		return new JacksonJsonDeserializer<>(new TypeReference<EventEnvelope<PaymentRequestedPayload>>() { }, strictJsonMapper())
			.trustedPackages("com.ssafy.kafkaorderlab.event")
			.ignoreTypeHeaders();
	}

	/**
	 * 주문 생성 이벤트 전용 consumer factory를 생성한다.
	 *
	 * @param kafkaProperties Spring Kafka 연결 설정
	 * @return 주문 생성 이벤트를 역직렬화하는 consumer factory
	 */
	@Bean
	public ConsumerFactory<String, EventEnvelope<OrderCreatedPayload>> orderCreatedConsumerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
		return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), orderCreatedValueDeserializer());
	}

	/**
	 * 결제 요청 이벤트 전용 consumer factory를 생성한다.
	 *
	 * @param kafkaProperties Spring Kafka 연결 설정
	 * @return 결제 요청 이벤트를 역직렬화하는 consumer factory
	 */
	@Bean
	public ConsumerFactory<String, EventEnvelope<PaymentRequestedPayload>> paymentRequestedConsumerFactory(KafkaProperties kafkaProperties) {
		Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
		return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), paymentRequestedValueDeserializer());
	}

	/**
	 * 주문 생성 이벤트 listener가 사용할 container factory를 생성한다.
	 *
	 * @param consumerFactory 주문 생성 이벤트 consumer factory
	 * @param partitionsLogger partition 할당 변화를 기록하는 rebalance listener
	 * @param autoStartup listener container 자동 시작 여부
	 * @return typed Kafka listener container factory
	 */
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<OrderCreatedPayload>> orderCreatedListenerContainerFactory(
		ConsumerFactory<String, EventEnvelope<OrderCreatedPayload>> consumerFactory,
		ConsumerAwareRebalanceListener partitionsLogger,
		@Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup
	) {
		return createListenerContainerFactory(consumerFactory, partitionsLogger, autoStartup);
	}

	/**
	 * 결제 요청 이벤트 listener가 사용할 container factory를 생성한다.
	 *
	 * @param consumerFactory 결제 요청 이벤트 consumer factory
	 * @param partitionsLogger partition 할당 변화를 기록하는 rebalance listener
	 * @param autoStartup listener container 자동 시작 여부
	 * @return typed Kafka listener container factory
	 */
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<PaymentRequestedPayload>> paymentRequestedListenerContainerFactory(
		ConsumerFactory<String, EventEnvelope<PaymentRequestedPayload>> consumerFactory,
		ConsumerAwareRebalanceListener partitionsLogger,
		@Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup
	) {
		return createListenerContainerFactory(consumerFactory, partitionsLogger, autoStartup);
	}

	private <T> ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<T>> createListenerContainerFactory(
		ConsumerFactory<String, EventEnvelope<T>> consumerFactory,
		ConsumerAwareRebalanceListener partitionsLogger,
		boolean autoStartup
	) {
		ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<T>> factory =
			new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setAutoStartup(autoStartup);
		factory.getContainerProperties().setConsumerRebalanceListener(partitionsLogger);
		return factory;
	}

	private JsonMapper strictJsonMapper() {
		return JsonMapper.builder()
			.withCoercionConfig(LogicalType.Integer,
				config -> config.setCoercion(CoercionInputShape.String, CoercionAction.Fail))
			.build();
	}
}
