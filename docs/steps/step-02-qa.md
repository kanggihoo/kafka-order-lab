---
title: Step 02 Q&A — Partition, key, consumer group
step: 02
---

# Step 02 Q&A — Partition, key, consumer group

Step 02에서 추가된 `AssignedPartitionsLogger`, consumer/container factory의 역할 분리, `containerFactory` 지정 방식, 그리고 주문 → 결제 이벤트 연쇄의 발행 시점을 정리한다.

## 1. `AssignedPartitionsLogger`는 무엇을 하는가?

```java
@Component
@Slf4j
class AssignedPartitionsLogger implements ConsumerAwareRebalanceListener {

	@Override
	public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		log.info("Assigned partitions: group={}, memberId={}, partitions={}",
			consumer.groupMetadata().groupId(), consumer.groupMetadata().memberId(), partitions);
	}

	@Override
	public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		log.info("Revoked partitions: group={}, memberId={}, partitions={}",
			consumer.groupMetadata().groupId(), consumer.groupMetadata().memberId(), partitions);
	}
}
```

이해한 내용이 맞다. `ConsumerAwareRebalanceListener`는 **rebalance가 일어날 때 호출되는 콜백들을 정의하는 인터페이스**이고, 이 클래스는 그중 두 개만 구현해서 **로그만 남긴다**. 상태를 바꾸거나 offset을 조작하지 않는다.

이 클래스가 step-02에 필요한 이유는 명확하다. 이번 step의 목표가 "consumer 수에 따른 partition 할당 변화를 재현"하는 것인데, 그 변화는 broker 내부에서 일어나므로 애플리케이션이 직접 찍어주지 않으면 관찰할 수 없다. 실제로 이 로그가 실험의 근거가 됐다.

```text
Revoked partitions: group=payment-service, partitions=[order-events-0, order-events-1, order-events-2]
Assigned partitions: group=payment-service, partitions=[order-events-2]
```

`ConsumerAware...`라는 이름이 붙은 이유는 콜백 파라미터로 `Consumer` 객체를 함께 받기 때문이다. 그래서 `consumer.groupMetadata()`로 어느 group의 어느 member인지 찍을 수 있다. 기본 Kafka 인터페이스인 `ConsumerRebalanceListener`는 partition 목록만 주기 때문에, 인스턴스가 4개일 때 "이 로그가 누구 것인지" 구분할 수 없다.

### `onPartitionsRevokedBeforeCommit`은 무슨 이벤트인가?

이게 질문에서 모르겠다고 한 부분이다. **partition 소유권을 빼앗기는 시점에 호출되는데, offset commit 전에 호출된다**는 뜻이다. Spring Kafka는 revoke 콜백을 두 개로 쪼개 놓았다.

| 콜백 | 호출 시점 |
|---|---|
| `onPartitionsRevokedBeforeCommit` | partition을 잃기 직전, **아직 offset commit이 안 된 상태** |
| `onPartitionsRevokedAfterCommit` | offset commit이 끝난 뒤, partition을 실제로 놓는 시점 |

왜 나눠 놓았는가. rebalance가 시작되면 내가 갖고 있던 partition은 곧 다른 consumer에게 넘어간다. 이때 "여기까지 처리했다"는 offset을 commit하지 않고 넘기면, 새 소유자가 이전 offset부터 다시 읽어서 **중복 처리**가 생긴다. 그래서 마지막 commit을 할 마지막 기회가 필요하고, 그 지점이 `BeforeCommit`이다.

즉 이 hook은 원래 **"partition을 놓기 전에 offset을 확실히 commit하거나, 처리 중인 작업을 마무리하라"**는 용도다. 다만 이번 step에서는 그런 처리를 하지 않고 로그만 찍는다. commit 시점 통제는 step-03의 주제이므로, 지금은 "revoke가 언제 일어나는지" 관찰용으로만 쓴다.

> 참고: 지금은 auto commit이라 commit 시점을 우리가 통제하지 않는다. step-03에서 수동 commit으로 바꾸면 이 hook이 중복 방지에 실제로 쓰이는 자리가 된다.

### 이 Bean이 어떻게 연결되는가

`@Component`로 등록된 뒤, container factory가 주입받아 container properties에 꽂는다.

```java
factory.getContainerProperties().setConsumerRebalanceListener(partitionsLogger);
```

그래서 이 factory를 쓰는 모든 listener(`order-observer`, `payment-service`, `payment-observer`)가 같은 logger를 공유한다. 로그에 `group=`을 찍는 이유가 여기 있다. 세 group이 같은 인스턴스를 쓰므로 group 이름 없이는 구분이 안 된다.

## 2. `ConsumerFactory`와 `ConcurrentKafkaListenerContainerFactory`의 차이

한 문장으로: **`ConsumerFactory`는 `KafkaConsumer` 객체를 만들고, container factory는 그 consumer를 돌리는 스레드와 `@KafkaListener` 메서드 호출 구조를 만든다.**

| 구분 | `ConsumerFactory` | `ConcurrentKafkaListenerContainerFactory` |
|---|---|---|
| 만드는 것 | `KafkaConsumer` 인스턴스 | listener container (poll 루프 + 스레드) |
| 담당 | 접속 정보, key/value deserializer | concurrency, rebalance listener, ack 모드, 에러 핸들러 |
| 관심사 | **"record를 어떻게 읽고 객체로 바꾸는가"** | **"몇 개 스레드로 언제 소비하고, 누구 메서드를 부르는가"** |

계층 관계는 다음과 같다.

```text
ConcurrentKafkaListenerContainerFactory   (컨테이너를 만든다)
  └─ concurrency=N 만큼 KafkaMessageListenerContainer 생성
       └─ 각 컨테이너가 ConsumerFactory로 KafkaConsumer 1개씩 생성
            └─ poll() 루프를 돌며 @KafkaListener 메서드 호출
```

이 구조가 step-02의 실험과 직결된다. `PAYMENT_SERVICE_CONCURRENCY=4`로 두면 container factory가 컨테이너 4개를 만들고, 각각 `ConsumerFactory`로 `KafkaConsumer`를 하나씩 만든다. 그 결과 **애플리케이션 하나가 group member 4개로 참여**하고, partition이 3개뿐이라 하나는 `#PARTITIONS 0`이 된다. 즉 "concurrency는 인스턴스 안의 consumer 수"라는 결론이 이 계층에서 나온다.

역할이 나뉘어 있으니 조합도 자유롭다. 이 프로젝트에서는 `orderCreatedConsumerFactory`와 `paymentRequestedConsumerFactory`가 **역직렬화 타입만 다르고**, container factory 생성 로직은 `createListenerContainerFactory()` private 메서드로 공유한다.

## 3. coercion 설정은 삭제되지 않았다

질문의 전제가 사실과 다르다. **삭제된 게 아니라 `strictJsonMapper()`로 추출됐다.** 두 deserializer가 같은 설정을 쓰기 때문이다.

```java
private JsonMapper strictJsonMapper() {
	return JsonMapper.builder()
		.withCoercionConfig(LogicalType.Integer,
			config -> config.setCoercion(CoercionInputShape.String, CoercionAction.Fail))
		.build();
}
```

`git diff e98fad6 f1b5fa3`로 확인한 실제 변경이다.

```diff
 	public JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>> orderCreatedValueDeserializer() {
-		JsonMapper mapper = JsonMapper.builder()
-			.withCoercionConfig(LogicalType.Integer,
-				config -> config.setCoercion(CoercionInputShape.String, CoercionAction.Fail))
-			.build();
-		return new JacksonJsonDeserializer<>(new TypeReference<...>() { }, mapper)
+		return new JacksonJsonDeserializer<>(new TypeReference<...>() { }, strictJsonMapper())
 			.trustedPackages("com.ssafy.kafkaorderlab.event")
 			.ignoreTypeHeaders();
 	}
```

메서드 안에 인라인으로 있던 코드가 메서드 밖으로 나갔을 뿐, `withCoercionConfig`, `trustedPackages`, `ignoreTypeHeaders`는 그대로다. step-02에서 `PaymentRequested` deserializer가 추가되면서 같은 mapper 설정이 두 곳에서 필요해졌고, 그래서 공통 메서드로 뽑았다. 결제 이벤트도 주문 이벤트와 **같은 엄격함**(문자열 → 숫자 자동 변환 거부)을 갖는다.

step-01의 `amount` 문자열 실패 실험은 지금도 동일하게 재현된다. `KafkaJsonConfigurationTest` 4개가 이 설정을 검증하고 있고, 이번 재검증에서 전체 23개 테스트가 통과했다.

## 4. `containerFactory`를 하드코딩해야 하는가?

먼저 사실 확인. step-01에서 `@Bean(name = "kafkaListenerContainerFactory")`로 이름을 준 것은 "구분"이 아니라 그 반대다. **Spring Kafka가 찾는 기본 이름**이 정확히 `kafkaListenerContainerFactory`이기 때문에, 그 이름으로 등록하면 `@KafkaListener`에서 `containerFactory`를 생략할 수 있었다. step-01은 container factory가 하나뿐이라 이 방식이 통했다.

step-02에서는 factory가 **두 개**가 됐다. `EventEnvelope<OrderCreatedPayload>`용과 `EventEnvelope<PaymentRequestedPayload>`용이다. 이 둘은 역직렬화 목표 타입이 다르므로 서로 바꿔 쓸 수 없다. `payment-events`를 주문용 factory로 읽으면 `PaymentRequested` JSON을 `OrderCreatedPayload`로 역직렬화하려다 실패한다.

그래서 어느 쪽을 쓸지 **명시할 수밖에 없다**. 현재 세 listener의 지정은 이렇게 갈린다.

| listener | group | topic | containerFactory |
|---|---|---|---|
| `OrderEventObserver` | `order-observer` | `order-events` | `orderCreatedListenerContainerFactory` |
| `PaymentRequestListener` | `payment-service` | `order-events` | `orderCreatedListenerContainerFactory` |
| `PaymentEventObserver` | `payment-observer` | `payment-events` | `paymentRequestedListenerContainerFactory` |

앞의 두 개는 같은 topic을 다른 group으로 읽으니 factory가 같고, 마지막만 다르다.

"하드코딩"을 줄이고 싶다면 선택지는 두 가지다.

1. **둘 중 많이 쓰는 쪽을 `kafkaListenerContainerFactory`라는 이름으로 등록**한다. 그러면 listener 3곳 중 2곳에서 `containerFactory`를 생략할 수 있고, `payment-observer`만 명시한다. 다만 생략된 listener는 어떤 factory를 쓰는지 코드에서 안 보인다.
2. **지금처럼 세 곳 모두 명시**한다. 문자열 상수라 오타 시 런타임에야 실패하는 단점이 있지만, listener만 봐도 어떤 타입 계약으로 읽는지 드러난다.

현재 코드는 2번이다. listener가 3개, factory가 2개로 갈리는 상황에서는 명시적인 쪽이 읽기 쉽다고 판단했다. 오타 위험이 걱정되면 factory 이름을 상수로 뽑을 수 있지만, `@KafkaListener` 속성은 컴파일 타임 상수만 받으므로 `public static final String`이어야 한다.

## 5. 결제 이벤트는 언제 발행되는가?

구조 이해가 맞다. 발행 지점은 **`PaymentRequestListener.requestPayment()`가 `OrderCreated`를 소비하는 순간**이고, HTTP 요청과는 분리된 별도 스레드다.

```java
@KafkaListener(
	topics = "${app.kafka.topics.order-events}",
	groupId = "${app.kafka.consumer.payment-service.group-id}",
	...
)
void requestPayment(ConsumerRecord<String, EventEnvelope<OrderCreatedPayload>> record) {
	validator.validate(record.value());
	OrderCreatedPayload payload = record.value().payload();
	log.info("order created consumed by payment-service: ...");
	paymentEventService.publishPaymentRequested(payload.orderId(), payload.amount());  // ← 여기
}
```

전체 흐름은 다음과 같다.

```text
POST /orders
  → OrderEventService가 order-events에 OrderCreated 발행 (key=orderId)
  → 즉시 202 Accepted 응답            ← 여기서 HTTP 요청은 끝난다
  ...
  → payment-service group의 consumer가 OrderCreated를 poll
  → 검증 후 PaymentEventService.publishPaymentRequested() 호출
  → payment-events에 PaymentRequested 발행 (key=같은 orderId)
  → payment-observer group이 그것을 소비해 로그
```

`order-events`를 읽는 group이 두 개(`order-observer`, `payment-service`)라는 점이 중요하다. 같은 record를 **각 group이 독립적으로 한 번씩** 받는다. `order-observer`는 로그만 남기고, `payment-service`만 결제 이벤트를 발행한다. consumer group이 다르면 offset도 따로 관리되므로 서로 영향이 없다.

key 선택도 의도적이다. `publishPaymentRequested()`가 `orderId`를 그대로 record key로 쓴다.

```java
ProducerRecord<> record = new ProducerRecord<>(paymentEventsTopic, orderId, event);
```

두 topic이 모두 3 partition이고 같은 key를 쓰므로, 같은 주문은 두 topic에서 **같은 partition 번호**를 받는다. 실험에서 12건 모두 `order-events`와 `payment-events`의 partition 번호가 일치한 이유다.

### 주의할 점: 발행 실패가 조용히 넘어간다

`kafkaTemplate.send()`는 비동기이고, 콜백에서 실패를 로그만 찍는다.

```java
future.whenComplete((result, error) -> {
	if (error != null) {
		log.error("payment event publish failed: ...", error);
		return;   // ← 재시도 없이 종료
	}
	...
});
```

즉 `OrderCreated`는 소비됐는데 `PaymentRequested` 발행이 실패하면 **결제 요청이 사라진다**. 게다가 auto commit이므로 offset은 그대로 진행되어 재처리도 안 된다. 이건 버그가 아니라 step-02의 의도된 범위 밖이고, 재시도·DLT는 step-07의 주제다. summary의 "아직 보장하지 못하는 것"에 명시해 두었다.

## 정리

| 질문 | 답 |
|---|---|
| `AssignedPartitionsLogger` | rebalance 콜백을 구현해 group/member/partition을 로그로만 남긴다. 이해한 내용이 맞다. |
| `onPartitionsRevokedBeforeCommit` | partition을 잃기 직전, offset commit 전 호출. 원래는 마지막 commit 기회. 지금은 관찰용. |
| consumer factory vs container factory | 전자는 `KafkaConsumer`를 만들고, 후자는 그것을 돌릴 스레드와 listener 호출 구조를 만든다. |
| coercion 설정 | 삭제되지 않았다. `strictJsonMapper()`로 추출해 두 deserializer가 공유한다. |
| `containerFactory` 하드코딩 | factory가 타입별로 2개가 되어 명시가 필요해졌다. 기본 이름을 쓰면 일부는 생략 가능. |
| 결제 이벤트 발행 시점 | `payment-service` group이 `OrderCreated`를 소비하는 순간. HTTP 응답 이후의 별도 스레드. |
