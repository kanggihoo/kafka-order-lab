# Step 01 Q&A — 이벤트 계약과 JSON 직렬화

Step 01의 핵심은 `String` 임시 payload를 타입 있는 `EventEnvelope<T>` 계약으로 바꾸고, Producer의 JSON 직렬화와 Consumer의 JSON 역직렬화를 각각 설정으로 분리하는 것이다.

## 1. 직렬화/역직렬화의 "세부 설정"은 무엇을 의미하는가?

```java
public JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> orderCreatedValueDeserializer() {
    JsonMapper mapper = JsonMapper.builder()
        .withCoercionConfig(LogicalType.Integer,
            config -> config.setCoercion(CoercionInputShape.String, CoercionAction.Fail))
        .build();
    return new JacksonJsonDeserializer<>(new TypeReference<EventEnvelope<OrderCreatedPayload>>() { }, mapper)
        .trustedPackages("com.ssafy.kafkaorderlab.event")
        .ignoreTypeHeaders();
}
```

`new JacksonJsonDeserializer<>(typeRef)`만 쓰면 "JSON 문자열을 자바 객체로 바꾼다"는 기본 동작만 한다. 여기서 말하는 세부 설정은 그 변환 과정에서 **무엇을 관대하게 허용하고 무엇을 엄격하게 거부할지, 어떤 타입까지 신뢰할지**를 정하는 부분이다.

### coercion 설정

- `LogicalType.Integer`: 대상 필드 타입 카테고리(정수 계열)
- `CoercionInputShape.String`: 실제 JSON에 들어온 값의 모양이 문자열인 경우
- `CoercionAction.Fail`: 그 경우 자동 변환하지 말고 예외를 던짐

이 설정이 없으면 Jackson은 기본적으로 `"amount": "15000"`(문자열)도 숫자로 알아서 강제 변환(coercion)한다. 그러면 실패 실험에서 `amount`를 문자열로 바꿔도 역직렬화가 조용히 성공해버려서, "필드 타입 변경은 계약을 깨뜨린다"는 step-01의 목표를 검증할 수 없다. `CoercionAction.Fail`은 이 자동 변환을 명시적으로 차단한다.

### trustedPackages / ignoreTypeHeaders

- `trustedPackages("com.ssafy.kafkaorderlab.event")`: 역직렬화 대상 타입을 이 패키지 하위로 제한한다. Kafka header에 담긴 타입 정보를 그대로 믿고 임의 클래스를 역직렬화하면 보안 문제가 될 수 있어서 범위를 좁힌다.
- `ignoreTypeHeaders()`: Kafka record header의 `__TypeId__` 같은 타입 힌트를 무시하고, 코드에 고정한 `TypeReference<EventEnvelope<OrderCreatedPayload>>`만 신뢰해서 역직렬화한다.

즉 "직렬화/역직렬화 설정"은 변환 자체가 아니라 **변환 규칙(무엇을 허용/거부할지, 무엇을 신뢰할지)**을 정하는 것이다.

## 2. `DefaultKafkaConsumerFactory`와 `ConcurrentKafkaListenerContainerFactory`는 무엇인가?

```java
@Bean
public ConsumerFactory<String, EventEnvelope<OrderCreatedPayload>> orderCreatedConsumerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
    return new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), orderCreatedValueDeserializer());
}

@Bean(name = "kafkaListenerContainerFactory")
public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<OrderCreatedPayload>> kafkaListenerContainerFactory(
    ConsumerFactory<String, EventEnvelope<OrderCreatedPayload>> consumerFactory,
    @Value("${spring.kafka.listener.auto-startup:true}") boolean autoStartup
) {
    ConcurrentKafkaListenerContainerFactory<String, EventEnvelope<OrderCreatedPayload>> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setAutoStartup(autoStartup);
    return factory;
}
```

- **`ConsumerFactory`**: `KafkaConsumer` 객체를 만드는 역할만 하는 인터페이스. `DefaultKafkaConsumerFactory`는 그 기본 구현체로, consumer 속성 맵과 key/value deserializer를 들고 있다가 요청이 오면 `KafkaConsumer` 인스턴스를 생성한다.
- **`ConcurrentKafkaListenerContainerFactory`**: `@KafkaListener` 메서드가 실제로 실행되는 "리스너 컨테이너"를 만드는 factory. "Concurrent"인 이유는 같은 consumer group 안에서 여러 consumer 스레드(파티션 병렬 처리)를 띄울 수 있기 때문이다. 내부적으로 `ConsumerFactory`를 사용해 각 스레드용 `KafkaConsumer`를 만든다.

```text
DefaultKafkaConsumerFactory  ──(consumer 생성 위임)──▶  KafkaConsumer
        ▲
        │ 주입
ConcurrentKafkaListenerContainerFactory ──▶ 리스너 컨테이너(스레드) 생성 ──▶ @KafkaListener 메서드 호출
```

하나는 "consumer를 만드는 공장", 다른 하나는 "그 공장을 사용해 리스너 실행 환경을 만드는 공장"이다.

## 3. `KafkaTemplate`은 이전 config 설정과 어떻게 연결되는가?

Producer 쪽 `KafkaTemplate`(`OrderEventService`가 주입받는 것)은 `KafkaJsonConfiguration`과 직접 연결되어 있지 않다. `application.yml`을 통해 Spring Boot가 자동 구성한다.

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JacksonJsonSerializer
```

Spring Boot는 이 설정으로 `ProducerFactory`와 `KafkaTemplate<?, ?>` 빈을 자동 구성한다. `OrderEventService`는 `KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>>` 타입으로 주입받는데, Boot가 만든 빈이 와일드카드 제네릭(`<?, ?>`)이라 Spring이 구체 타입 요청에도 매칭해서 그대로 주입된다.

```java
public OrderEventService(
    KafkaTemplate<String, EventEnvelope<OrderCreatedPayload>> kafkaTemplate,
    @Value("${app.kafka.topics.order-events}") String orderEventsTopic
) { ... }
```

Consumer 쪽(`KafkaJsonConfiguration`)은 반대로 수동 구성이다.

```java
Map<String, Object> properties = kafkaProperties.buildConsumerProperties();
new DefaultKafkaConsumerFactory<>(properties, new StringDeserializer(), orderCreatedValueDeserializer());
```

`kafkaProperties`가 `application.yml`의 `bootstrap-servers`, `consumer.auto-offset-reset` 등 공통 설정을 읽어오고, 거기에 커스텀 `JacksonJsonDeserializer`를 직접 꽂아서 `ConsumerFactory` 빈을 만든다. 이 빈이 `@Bean(name = "kafkaListenerContainerFactory")`로 등록된 `ConcurrentKafkaListenerContainerFactory`에 들어가고, `OrderEventObserver`의 `@KafkaListener`는 `containerFactory` 속성을 명시하지 않았지만 Spring이 기본으로 찾는 빈 이름이 정확히 `"kafkaListenerContainerFactory"`라서 이 커스텀 factory를 자동으로 사용한다. Boot의 기본 자동 구성 factory는 `@ConditionalOnMissingBean`이라 이 커스텀 빈이 이미 있으면 만들어지지 않는다.

| 구분 | 구성 방식 | 사용 설정 |
|---|---|---|
| Producer(`KafkaTemplate`) | Spring Boot 자동 구성 | `spring.kafka.producer.*` |
| Consumer(`ConsumerFactory`, container factory) | `KafkaJsonConfiguration`의 수동 `@Bean` | `spring.kafka.*`(공통) + 코드로 지정한 deserializer |

정리하면, producer는 `application.yml` 설정만으로 Boot가 자동 구성한 빈을 그대로 주입받아 쓰고, consumer는 같은 `application.yml` 값을 `KafkaProperties`로 읽되 value deserializer만 코드로 직접 갈아끼운 수동 구성이다.

## 최종 요약

Step 01의 직렬화 세부 설정(coercion, trustedPackages, ignoreTypeHeaders)은 "JSON을 객체로 바꾼다"는 기본 동작에 "무엇을 허용/거부하고 무엇을 신뢰할지"를 더하는 규칙이다. `DefaultKafkaConsumerFactory`는 `KafkaConsumer`를 만드는 공장이고 `ConcurrentKafkaListenerContainerFactory`는 그 공장을 사용해 `@KafkaListener`가 실행될 리스너 컨테이너를 만드는 상위 공장이다. `KafkaTemplate`은 Boot가 `application.yml`의 producer 설정으로 자동 구성한 빈을 그대로 주입받아 쓰는 반면, consumer factory와 listener container factory는 `KafkaJsonConfiguration`에서 같은 공통 설정 위에 커스텀 deserializer를 얹어 수동으로 구성한다.
