# Step 00 Q&A — Kafka 주문 이벤트 첫 연결

Step 00의 핵심은 HTTP 요청을 Kafka record 발행으로 연결하고, Producer의 저장 결과와 Consumer의 수신 결과를 구분해서 관찰하는 것이다.

## 1. `KafkaTemplate`은 무엇인가?

`KafkaTemplate`은 Spring에서 Kafka Producer를 사용하기 위한 추상화 클래스다. Redis의 `RedisTemplate`과 비슷하게 외부 시스템과 애플리케이션 사이의 사용 API를 제공하지만, Kafka topic에 record를 발행한다는 점이 다르다.

```java
KafkaTemplate<String, String>
```

- 첫 번째 `String`: Kafka key 타입
- 두 번째 `String`: Kafka value 타입

Spring Boot가 Kafka 설정을 바탕으로 `ProducerFactory`와 `KafkaTemplate`을 자동 구성하고, Service의 생성자에 Bean으로 주입한다.

### 질문: `KafkaTemplate`에서 Serializer 설정이 가능한가?

가능하다. 다만 정확히는 `KafkaTemplate` 자체가 Serializer를 직접 관리하는 것이 아니라, 내부의 `ProducerFactory`가 Kafka Producer와 Serializer를 구성한다.

기본적인 문자열 발행은 `application.yml`에서 설정할 수 있다.

```yaml
spring:
  kafka:
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

Custom Serializer, 여러 `KafkaTemplate`, Custom `ObjectMapper`가 필요할 때는 `@Configuration`에서 `ProducerFactory`와 `KafkaTemplate` Bean을 직접 구성할 수 있다. 단순한 문자열 발행만 필요한 Step 00에서는 별도 Configuration Bean을 만들 필요가 없다.

## 2. 직접 JSON으로 변환해야 하는가?

현재 코드는 다음 두 단계를 사용한다.

```text
OrderCreatedEvent
    ↓ ObjectMapper
JSON String
    ↓ Kafka StringSerializer
byte[]
    ↓
Kafka Broker
```

```java
String event = toJson(new OrderCreatedEvent(...));
kafkaTemplate.send(orderEventsTopic, request.orderId(), event);
```

이 방식은 Step 00에서 문자열 JSON의 흐름을 보여주기 위한 임시 구현이다.

Kafka JSON Serializer를 사용하면 애플리케이션이 직접 `toJson()`을 호출하지 않아도 된다.

```java
KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

kafkaTemplate.send(
    orderEventsTopic,
    request.orderId(),
    new OrderCreatedEvent(request.orderId(), request.amount(), "OrderCreated")
);
```

이 경우 Producer의 value serializer가 `OrderCreatedEvent`를 JSON byte 배열로 변환한다. Consumer 쪽에는 대응하는 JSON Deserializer 또는 Message Converter가 필요하다.

따라서 결론은 다음과 같다.

- 직접 `ObjectMapper` 변환: Step 00의 단순한 임시 방식
- Kafka JSON Serializer: 이후 이벤트 계약을 정식화할 때 적합한 방식
- Serializer와 Deserializer는 Producer와 Consumer 양쪽의 계약이므로 한쪽만 변경하면 안 된다.

## 3. `send()`와 `whenComplete()`는 무엇을 의미하는가?

현재 발행 흐름은 다음과 같다.

```java
CompletableFuture<SendResult<String, String>> future =
    kafkaTemplate.send(orderEventsTopic, request.orderId(), event);

future.whenComplete((result, error) -> {
    // 발행 성공 또는 실패 결과 처리
});
```

### `send()`

`send()`는 Kafka Producer에게 특정 topic으로 record를 발행하라는 요청이다. 현재 호출은 다음 정보를 사용한다.

```java
kafkaTemplate.send(topic, key, value);
```

- topic: `order-events`
- key: `orderId`
- value: JSON 문자열

`send()`가 완료되었다는 것은 Consumer가 업무 처리를 끝냈다는 의미가 아니다. Producer가 Kafka Broker에 record를 전달하고 저장 응답을 받는 흐름이다.

### `whenComplete()`

`whenComplete()`는 `CompletableFuture`가 성공 또는 실패로 완료될 때 실행할 callback을 등록한다.

- 성공: Broker가 record를 저장하고 Producer에 응답
- 실패: 연결 실패, timeout, serialization 오류 등

성공 callback의 `RecordMetadata`는 Consumer 결과가 아니다.

```java
RecordMetadata metadata = result.getRecordMetadata();
```

`RecordMetadata`에는 Broker가 할당한 topic, partition, offset 등이 들어 있다. 즉 Producer 관점에서 record가 어디에 저장됐는지 나타낸다.

### 비동기로 동작하는가?

현재 코드는 명시적으로 기다리지 않으므로 일반적인 성공 상황에서는 비동기로 동작한다. `future.get()`이나 `join()`을 호출하지 않기 때문에 callback 완료까지 HTTP 요청이 기다리지 않는다.

```text
send() 호출
    ↓
Future 반환
    ↓
HTTP 202 응답
    ↓
나중에 Broker 응답 수신
    ↓
whenComplete() 실행
```

다만 비동기라는 말이 `send()`가 항상 즉시 반환된다는 뜻은 아니다. Broker가 꺼져 있거나 Producer가 metadata를 가져오지 못하면 `send()` 내부에서 잠시 대기할 수 있다. Step 00의 Broker 중지 실험에서 HTTP 응답이 지연된 이유가 이것이다.

현재 HTTP `202 Accepted`의 의미는 “Kafka 발행 요청을 접수했다”는 것이며, “Broker 저장이 최종 성공했다”는 의미는 아니다.

## 4. `@KafkaListener`는 언제 실행되는가?

```java
@KafkaListener(
    topics = "${app.kafka.topics.order-events}",
    groupId = "${app.kafka.consumer.group-id}"
)
void observe(ConsumerRecord<String, String> record) {
    log.info("...");
}
```

애플리케이션이 시작되면 Spring이 이 메서드를 발견하고 Listener Container를 백그라운드에서 실행한다. Listener Container는 Kafka를 계속 polling하다가 record를 발견하면 `observe()`를 호출한다.

```text
애플리케이션 시작
    ↓
Consumer Group으로 Kafka polling 시작
    ↓
order-events에서 record 발견
    ↓
observe(record) 호출
    ↓
현재 구현에서는 key, partition, offset, value 로그 기록
```

### `topics`

Consumer가 구독할 topic이다. `@KafkaListener`의 `topics`는 여러 topic을 받을 수 있지만, 현재 Step 00에서는 `order-events` 하나만 사용한다.

### `groupId`

Consumer Group의 이름이다. Kafka는 group 단위로 partition과 offset을 관리한다.

- 같은 group의 Consumer: record를 나누어 처리
- 다른 group의 Consumer: 같은 record를 각각 읽을 수 있음

`groupId`가 하나라고 해서 Kafka 전체에 Consumer Group이 하나만 존재하는 것은 아니다. 하나의 listener endpoint가 참여하는 group이 하나라는 의미다.

### `auto-offset-reset: earliest`

해당 Consumer Group에 저장된 offset이 없을 때 가장 오래된 record부터 읽도록 한다. 이미 offset이 저장되어 있으면 기존 offset부터 이어서 읽는다.

## 5. topic과 group ID를 Java 코드에 하드코딩해야 하는가?

하드코딩할 필요가 없다. 현재는 `application.yml`로 분리했다.

```yaml
app:
  kafka:
    topics:
      order-events: order-events
    consumer:
      group-id: order-observer
```

Producer도 같은 설정값을 사용한다.

```java
private final String orderEventsTopic;

public OrderEventService(
    KafkaTemplate<String, String> kafkaTemplate,
    ObjectMapper objectMapper,
    @Value("${app.kafka.topics.order-events}") String orderEventsTopic
) {
    this.orderEventsTopic = orderEventsTopic;
}
```

따라서 Producer와 Consumer가 서로 다른 문자열을 사용하는 실수를 줄일 수 있다.

다만 topic을 실제 Kafka에 생성하는 `docker-compose.yml`의 `kafka-init` 명령에도 `order-events`가 있다. topic 이름을 변경하면 애플리케이션 설정과 topic 생성 설정을 함께 변경해야 한다.

## 6. `docker-compose.yml`의 `kafka`와 `kafka-init`은 무엇이 다른가?

둘 다 `apache/kafka:4.1.2` 이미지를 사용하지만 역할이 다르다.

| 서비스 | 역할 | 실행 결과 |
|---|---|---|
| `kafka` | 실제 Kafka Broker와 Controller 실행 | 계속 실행 |
| `kafka-init` | Kafka CLI로 topic 생성 | 명령 완료 후 종료 |

`kafka-init`은 Kafka Broker를 하나 더 실행하는 것이 아니다. Kafka 이미지 안에 있는 `kafka-topics.sh` 명령을 사용해 topic을 준비하는 일회성 초기화 작업이다.

```yaml
depends_on:
  kafka:
    condition: service_healthy
```

Kafka healthcheck가 통과한 뒤 topic 생성 명령을 실행한다.

```yaml
command: >-
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists
  --topic order-events --partitions 1 --replication-factor 1
```

### `command`가 컨테이너를 종료시키는가?

`command` 자체가 종료 옵션인 것은 아니다. Docker Compose의 `command`는 이미지의 기본 실행 명령을 덮어쓴다.

현재 명령은 topic을 생성한 뒤 프로세스가 종료된다. Docker 컨테이너는 메인 프로세스가 종료되면 함께 종료되므로 `kafka-init`도 종료된다.

```text
kafka-topics.sh 실행
    ↓
topic 생성
    ↓
명령 프로세스 종료
    ↓
kafka-init 컨테이너 종료
```

종료되는 것은 정상이며, Kafka Broker인 `kafka` 서비스는 별도의 장기 실행 프로세스를 사용하므로 계속 실행된다.

### `>-`는 무엇인가?

YAML의 여러 줄 문자열 문법이다.

- `>`: 줄바꿈을 공백으로 합침
- `-`: 마지막 줄바꿈 문자를 제거

따라서 여러 줄로 작성한 `command`는 실제로 하나의 긴 명령어로 실행된다.

## 7. `application.yml`과 Docker의 접속 주소는 왜 다른가?

현재 Spring Boot 애플리케이션은 호스트에서 실행하고 Kafka는 Docker에서 실행한다.

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
```

호스트에서 실행되는 Spring Boot는 `localhost:29092`로 접속한다. Docker 컨테이너 내부에서 실행되는 `kafka-init`은 Docker 서비스 이름을 사용해 `kafka:9092`로 접속한다.

| 실행 위치 | Kafka 주소 |
|---|---|
| 호스트의 Spring Boot | `localhost:29092` |
| Docker 네트워크 내부 컨테이너 | `kafka:9092` |

`KAFKA_ADVERTISED_LISTENERS`에 두 주소를 모두 등록한 이유도 실행 위치에 따라 올바른 주소를 제공하기 위해서다.

## 최종 요약

Step 00의 Producer 흐름은 `KafkaTemplate.send()`로 Broker에 발행을 요청하고, `whenComplete()`에서 Broker 저장 결과를 확인하는 구조다. 이 callback은 Consumer의 업무 처리 완료를 의미하지 않으며, Consumer는 `@KafkaListener`가 별도의 백그라운드 polling으로 처리한다. topic과 group ID는 설정으로 분리하고, `kafka-init`은 Broker가 아니라 topic을 한 번 생성하는 초기화 컨테이너다. 현재는 문자열 JSON을 사용하지만 다음 단계에서는 Kafka JSON Serializer와 Deserializer를 사용하도록 확장할 수 있다.
