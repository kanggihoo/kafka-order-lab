# Step 01 — 이벤트 계약과 JSON 직렬화 실험

## 목적

`OrderCreated` 이벤트의 v1/v2 호환성과 잘못된 계약의 소비 실패를 확인한다. 현재는 retry/DLT를 설정하지 않았으므로 실패 record가 재전달될 수 있다.

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
- 실제: 미실행
- 결론: 미기록

## v2 발행

`couponCode`를 보내면 v2를 발행한다.

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"1002","amount":15000,"couponCode":"WELCOME"}'
```

- 예상: `202 Accepted`, `eventVersion: 2`, payload에 `couponCode: "WELCOME"`
- 실제: 미실행
- 결론: 미기록

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
- 실제: 미실행
- 결론: 미기록

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
- 실제: 미실행
- 결론: 미기록
