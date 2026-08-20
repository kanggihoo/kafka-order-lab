# Step 01 — 이벤트 계약과 JSON 직렬화 실험

## 목적

`OrderCreated` 이벤트의 v1/v2 호환성과 잘못된 계약의 소비 실패를 확인한다. 현재는 retry/DLT를 설정하지 않았으므로 실패 record가 재전달될 수 있다.

## 실행 상태

| 항목 | 내용 |
|---|---|
| 실행 일시 | 2026-08-20 |
| 실행 환경 | Java 21.0.11, Spring Boot 4.1.0, Apache Kafka 4.1.2, Docker Compose 5.2.0 |
| broker 상태 | `kafka-order-lab-kafka-1` healthy |
| topic 상태 | `order-events`, partition 0 |
| 최종 상태 | v1·v2 발행·소비, 문자열 `amount` 역직렬화 실패, `orderId` 검증 실패와 재시도 동작 확인 완료 |

## 사전 조건

```bash
docker compose up -d
./gradlew bootRun
```

다른 터미널에서 observer 로그를 확인한다. 로그에는 `eventId`, `eventType`, `eventVersion`, key, topic, partition, offset이 출력된다.

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-events \
  --from-beginning \
  --property print.key=true
```

## v1 발행

`couponCode`를 보내지 않으면 v1을 발행한다.

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"1001","amount":15000}'
```

- 예상: `202 Accepted`, `eventVersion: 1`, payload에 `couponCode` 없음
- 실제: Kafka를 재생성한 최종 실험에서 producer가 key=`1101`, partition=0, offset=0에 발행했다. observer가 같은 offset에서 `eventVersion=1`을 로그로 남겼고, record payload에는 `couponCode`가 없었다. header에는 `eventType:OrderCreated`, `eventVersion:1`이 포함됐다.
- 결론: `couponCode`가 없는 주문 요청은 v1 계약으로 직렬화된다.

## v2 발행

`couponCode`를 보내면 v2를 발행한다.

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"1002","amount":15000,"couponCode":"WELCOME"}'
```

- 예상: `202 Accepted`, `eventVersion: 2`, payload에 `couponCode: "WELCOME"`
- 실제: Kafka를 재생성한 최종 실험에서 producer가 key=`1102`, partition=0, offset=1에 발행했다. observer가 같은 offset에서 `eventVersion=2`를 로그로 남겼고, record payload에는 `couponCode: "WELCOME"`이 포함됐다. header에는 `eventType:OrderCreated`, `eventVersion:2`가 포함됐다.
- 결론: `couponCode`가 있는 주문 요청은 선택 필드가 추가된 v2 계약으로 직렬화된다.

자동화된 `OrderEventJsonIntegrationTest`는 실제 embedded Kafka에서 v1과 v2 JSON을 모두 역직렬화한다. legacy v1 consumer는 알 수 없는 `couponCode`를 무시하도록 설정해야 한다.

## 실패 실험: amount 타입 변경

`amount`를 문자열로 만든 raw JSON을 보낸다.

```bash
printf '%s\n' '1003:{"eventId":"7d1d1cb2-9b4d-4c67-b512-1e1d9f4c5290","eventType":"OrderCreated","eventVersion":1,"occurredAt":"2026-08-20T00:00:00Z","payload":{"orderId":"1003","amount":"15000"}}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic order-events \
      --property parse.key=true \
      --property key.separator=:
```

- 예상: `JacksonJsonDeserializer`가 숫자 필드의 문자열 강제 변환을 거부하고 listener 처리에 실패한다.
- 실제: key=`1201`, partition=0, offset=2에 raw JSON을 발행했다. `MismatchedInputException`이 발생해 문자열 `"15000"`을 `long`으로 변환하지 않았고, Spring Kafka는 `ErrorHandlingDeserializer`가 필요하다는 오류를 반복해서 출력했다. consumer는 계속 offset 2를 다시 읽어 뒤의 record로 진행하지 못했다.
- 결론: 타입이 깨진 poison pill은 listener 호출 전 역직렬화 단계에서 실패한다. 현재 `DefaultErrorHandler`는 `SerializationException`을 직접 처리하지 못하므로 offset 2가 무제한 반복되며, 이를 처리하려면 이후 단계에서 `ErrorHandlingDeserializer`와 복구 정책이 필요하다.

## 실패 실험: orderId 누락

`orderId`가 없는 raw JSON을 보낸다.

```bash
printf '%s\n' '1004:{"eventId":"d0fbfe1a-79db-4a13-8c94-4d3d9944b661","eventType":"OrderCreated","eventVersion":1,"occurredAt":"2026-08-20T00:00:00Z","payload":{"amount":15000}}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic order-events \
      --property parse.key=true \
      --property key.separator=:
```

- 예상: JSON은 envelope로 읽히지만 consumer의 계약 검증에서 `orderId is required` 예외가 발생한다.
- 실제: key=`1202`, partition=0, offset=3에 raw JSON을 발행했다. 앞선 poison pill을 분리하기 위해 애플리케이션을 중지하고 `order-observer` offset을 3으로 이동한 뒤 다시 실행했다. 역직렬화는 성공했지만 validator가 `InvalidOrderCreatedEventException: orderId is required`를 발생시켰다. 기본 error handler는 offset 3을 총 10회 시도한 뒤 exhausted 로그를 남기고 offset 4를 커밋했다. 최종 consumer group 상태는 current offset 4, log end offset 4, lag 0이었다.
- 결론: 의미 검증 실패는 listener 예외이므로 기본 error handler의 재시도 대상이 된다. 현재는 DLT가 없어 시도 소진 후 record가 별도 보관되지 않고 건너뛰어진다.

## 실행 중 발견하고 해결한 환경 문제

최초 실행에서는 단일 broker 구성인데 Kafka 내부 consumer offset topic의 replication factor를 1로 지정하지 않아 `order-observer`가 group coordinator를 찾지 못했다. producer는 consumer group 없이도 기록할 수 있어 이 문제가 늦게 드러났다.

Compose의 Kafka 환경 변수에 다음 설정을 추가하고 broker를 재생성했다.

```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

재생성 후 `order-observer`가 `order-events-0`을 할당받았으며 v1·v2 정상 처리와 두 실패 실험을 완료했다.

## 최종 관찰

| record | offset | 실패 위치 | 실제 반복·복구 동작 |
|---|---:|---|---|
| 정상 v1 | 0 | 없음 | observer 처리 성공 |
| 정상 v2 | 1 | 없음 | observer 처리 성공 |
| 문자열 `amount` | 2 | JSON 역직렬화 | 같은 offset에서 무제한 반복, 후속 record 차단 |
| 누락된 `orderId` | 3 | listener 계약 검증 | 총 10회 시도 후 DLT 없이 건너뛰고 offset 4 커밋 |
