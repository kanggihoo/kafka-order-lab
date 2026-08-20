# Step 02 Runbook — Partition, key, consumer group

`order-events`와 `payment-events`를 3 partition으로 운영하면서 key 라우팅과 consumer group 할당을 재현하는 실행 절차다. 결과는 `docs/experiments/step-02-partition-key-consumer-group.md`에 기록한다.

## 이번 단계의 구성 요약

| 항목 | 값 |
|---|---|
| topic | `order-events` (3 partition), `payment-events` (3 partition) |
| producer key | `orderId` |
| consumer group | `order-observer`, `payment-service`, `payment-observer` |
| 결제 흐름 | `order-events`의 `OrderCreated` → `payment-service` → `payment-events`의 `PaymentRequested` |
| 병렬성 조정 | `ORDER_OBSERVER_CONCURRENCY`, `PAYMENT_SERVICE_CONCURRENCY`, `PAYMENT_OBSERVER_CONCURRENCY` |
| 포트 조정 | `SERVER_PORT` |

## 1. topic 재생성

partition 수 변경은 기존 key 분포를 바꾸므로 실습 환경에서만 수행한다.

```bash
docker compose down -v
docker compose up -d
docker compose ps -a
```

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic order-events
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic payment-events
```

두 topic 모두 `PartitionCount: 3`이어야 한다.

## 2. 애플리케이션 실행과 partition 할당 관찰

인스턴스를 1개부터 4개까지 늘리며 각 인스턴스의 `Assigned partitions` 로그를 비교한다. 인스턴스마다 포트를 다르게 준다.

```bash
./gradlew bootJar
SERVER_PORT=8080 java -jar build/libs/kafka-order-lab-0.0.1-SNAPSHOT.jar
SERVER_PORT=8081 java -jar build/libs/kafka-order-lab-0.0.1-SNAPSHOT.jar
SERVER_PORT=8082 java -jar build/libs/kafka-order-lab-0.0.1-SNAPSHOT.jar
SERVER_PORT=8083 java -jar build/libs/kafka-order-lab-0.0.1-SNAPSHOT.jar
```

여러 인스턴스를 동시에 띄울 때는 `./gradlew bootRun`보다 jar 실행이 종료·재시작을 통제하기 쉽다. 인스턴스를 죽인 뒤에는 session timeout(기본 45초)이 지나야 group에서 member가 제거되므로, 할당을 다시 확인할 때는 잠시 기다린다.

각 인스턴스 로그에서 다음 형식을 확인한다.

```text
Assigned partitions: group=payment-service, memberId=..., partitions=[order-events-0, order-events-1, order-events-2]
Revoked partitions: group=payment-service, memberId=..., partitions=[...]
```

group 단위 할당은 CLI로도 확인한다.

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group payment-service --describe
```

기대 결과:

| 인스턴스 수 | `payment-service` 할당 |
|---:|---|
| 1 | 한 consumer가 P0, P1, P2 담당 |
| 2 | 2 : 1로 분배 |
| 3 | 각 consumer가 1개씩 담당 |
| 4 | 1개 consumer는 할당 없음(idle) |

## 3. 같은 key의 partition 고정 확인

`1001`에 같은 주문 이벤트를 여러 번 보내고 partition 번호가 고정되는지 본다.

```bash
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/orders \
    -H 'Content-Type: application/json' \
    -d '{"orderId":"1001","amount":15000}'
done
```

`1002`~`1010`은 여러 partition으로 분산되는지 본다.

```bash
for id in 1002 1003 1004 1005 1006 1007 1008 1009 1010; do
  curl -s -X POST http://localhost:8080/orders \
    -H 'Content-Type: application/json' \
    -d "{\"orderId\":\"$id\",\"amount\":15000}"
done
```

애플리케이션 로그에서 확인할 항목은 다음과 같다.

- `order event published: ... key=1001, topic=order-events, partition=?`
- `order created consumed by payment-service: key=1001, ... partition=?`
- `payment event published: ... key=1001, topic=payment-events, partition=?`
- `payment event observed: ... key=1001, topic=payment-events, partition=?`

partition 번호는 CLI로도 확인한다.

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic payment-events \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

같은 partition 수를 가진 두 topic에 같은 key로 보내면 `order-events`와 `payment-events`의 partition 번호가 같다. 이 규칙은 자동화된 `PartitionRoutingIntegrationTest`로도 검증한다.

```bash
./gradlew test --tests 'com.ssafy.kafkaorderlab.PartitionRoutingIntegrationTest'
```

## 4. 실패 실험: key를 null로 보내기

애플리케이션 producer는 항상 `orderId`를 key로 쓰므로, key 없는 record는 CLI로 직접 발행한다.

```bash
printf '%s\n' \
  '{"eventId":"7d1d1cb2-9b4d-4c67-b512-1e1d9f4c5290","eventType":"OrderCreated","eventVersion":1,"occurredAt":"2026-08-20T00:00:00Z","payload":{"orderId":"1001","amount":15000}}' \
  '{"eventId":"d0fbfe1a-79db-4a13-8c94-4d3d9944b661","eventType":"OrderCreated","eventVersion":1,"occurredAt":"2026-08-20T00:00:01Z","payload":{"orderId":"1001","amount":15000}}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:9092 \
      --topic order-events
```

- 예상: key가 없으므로 같은 `1001` 주문의 이벤트가 서로 다른 partition에 들어갈 수 있고, 주문 단위 순서 보장이 깨진다.
- 확인: observer 로그의 `key=null`과 partition 번호, 그리고 `payment-events`에 발행된 record의 partition 번호를 비교한다.

## 5. 실패 실험: concurrency를 partition 수보다 크게 설정

```bash
PAYMENT_SERVICE_CONCURRENCY=4 SERVER_PORT=8080 ./gradlew bootRun
```

- 예상: consumer container thread는 4개지만 partition이 3개이므로 한 thread는 할당을 받지 못한다. 추가 병렬성이 생기지 않는다.
- 확인: `Assigned partitions` 로그가 3개 thread에만 partition을 부여하고, 한 thread는 빈 목록 또는 할당 로그 없이 대기한다. `kafka-consumer-groups.sh --describe` 출력에서 `CONSUMER-ID`가 4개지만 partition은 3개에만 매핑된다.

## 6. 정리

```bash
docker compose down -v
```
