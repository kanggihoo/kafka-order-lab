---
title: Kafka 주문 처리 학습 상세 가이드
tags:
  - kafka
  - spring-boot
  - learning
  - experiments
aliases:
  - Kafka 상세 학습 가이드
---

# Kafka 주문 처리 학습 상세 가이드

이 문서는 [[Kafka 주문 처리 학습 로드맵]]의 각 단계를 실제 실험 단위로 나눈 가이드다.

## 공통 이벤트와 topic

```json
{
  "eventId": "uuid",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-08-19T10:00:00Z",
  "payload": { "orderId": 1001 }
}
```

| Topic | Key | 목적 | 정리 정책 |
|---|---|---|---|
| `order-events` | `orderId` | 주문 생명주기 이력 | delete/retention |
| `payment-events` | `orderId` | 결제 결과 이력 | delete/retention |
| `order-status` | `orderId` | 주문별 최신 상태 | compact |
| `order-events-retry-*` | 원본 key | 재시도 | delete/retention |
| `order-events-dlt` | 원본 key | 최종 실패 | delete/retention |
| `outbox-event` | `eventId` | Outbox/CDC 결과 | delete/retention |

## 0. 단일 Kafka와 Spring Boot 연결

### 학습할 것

- broker, topic, partition, record의 관계
- KRaft 기반 단일 Kafka 실행
- `KafkaTemplate.send()`와 `@KafkaListener`
- `key`, `partition`, `offset` 로그 기록

### 구현

```text
POST /api/orders
  → OrderCreated를 order-events에 발행
  → @KafkaListener에서 record를 수신
```

### 실험과 완료 기준

- `orderId=1001`을 key로 세 번 발행한다.
- 각 메시지의 topic, partition, offset을 로그로 남긴다.
- CLI consumer로도 같은 record를 읽어 본다.

완료: Spring 애플리케이션과 CLI 양쪽에서 같은 record의 partition/offset을 확인한다.

## 1. 메시지 계약과 직렬화

### 학습할 것

- Kafka key/value/header의 역할
- JSON serializer/deserializer
- `eventId`는 중복 제거 식별자, `eventVersion`은 계약 변경 식별자라는 점
- 역직렬화 실패는 일반 listener 예외와 처리 경로가 다를 수 있다는 점

### 실험

1. `OrderCreated` v1을 발행·수신한다.
2. optional 필드를 추가한 v2를 발행한다.
3. 기존 필드의 타입을 변경하거나 필수 필드를 제거해 consumer 실패를 확인한다.

완료: 하위 호환 변경과 호환되지 않는 변경을 구분할 수 있다.

## 2. Partition, key, consumer group

### 학습할 것

- partition은 병렬 처리 단위다.
- 순서는 topic 전체가 아니라 partition 안에서만 보장된다.
- 하나의 consumer group에서 partition 하나는 consumer 하나에만 할당된다.

### 실험

`order-events`를 3 partition으로 만든다.

```text
Consumer 1대  → P0, P1, P2 담당
Consumer 2대  → partition 분배
Consumer 3대  → 하나씩 분배
Consumer 4대  → 1대는 할당 없음
```

같은 `orderId`는 같은 partition으로, 다른 `orderId`는 여러 partition으로 보내 본다.

완료: 처리량을 늘리려면 consumer만이 아니라 partition 수를 함께 고려해야 함을 설명한다.

## 3. Offset, commit, rebalance, lag

### 학습할 것

- log end offset, committed offset, lag
- auto commit과 수동 commit
- consumer 시작/종료 시 rebalance
- 긴 처리 시간과 `max.poll.interval.ms`

### 필수 장애 실험

1. business 처리 성공 뒤, offset commit 전에 프로세스를 종료한다.
2. offset commit 뒤, business 처리 전에 프로세스를 종료한다.
3. listener에 지연을 넣고 producer를 계속 호출해 lag를 만든다.
4. consumer 인스턴스를 추가·종료해 partition 재할당을 확인한다.

> [!warning] 핵심 결론
> 처리 뒤 commit은 중복 가능성을 남기고, 처리 전 commit은 유실 가능성을 만든다. 이 긴장은 이후 Outbox와 idempotent consumer를 배우는 출발점이다.

완료: 같은 event가 왜 재수신될 수 있는지와 lag 계산 방식을 설명한다.

## 4. Retention과 compaction

### 학습할 것

- Kafka가 queue가 아니라 보관 가능한 log라는 점
- `retention.ms`, `retention.bytes`
- `cleanup.policy=delete`, `cleanup.policy=compact`
- tombstone(key가 있고 value가 null인 record)

### 실험

`order-status`에 다음을 발행한다.

```text
1001 → CREATED
1001 → PAID
1001 → COMPLETED
```

compaction 이후 최신 상태 중심으로 정리되는 것을 확인한다. compaction은 즉시 수행되지 않는 백그라운드 작업임도 기록한다.

완료: 이력용 `order-events`와 상태용 `order-status`를 분리하는 이유를 설명한다.

## 5. 관찰과 테스트

### 학습할 것

- `kafka-consumer-groups.sh`로 consumer group, lag 확인
- `kafka-topics.sh --describe`로 leader, replica, ISR 확인
- Actuator와 애플리케이션 로그
- Testcontainers 또는 통합 테스트

### 기록 템플릿

```md
## 실험: Consumer가 DB 처리 뒤 종료되면?

- 설정: `enable-auto-commit=false`
- 행동: DB 저장 직후 프로세스 강제 종료
- 예상: 동일 `eventId` 재수신
- 실제: 
- 결론: 
```

완료: 모든 주요 실험을 `docs/experiments/`에 한 개 이상 기록한다.

## 6. 3-node KRaft와 replication

### 학습할 것

- controller quorum과 controller leader
- partition leader와 follower replica
- replication factor, ISR, leader election

### 실험

```text
partition = 3
replication.factor = 3
```

한 broker를 중단하고 controller leader 및 partition leader 변화를 확인한다.

완료: controller leader와 partition leader가 다른 책임을 갖는다는 점을 설명한다.

## 7. acks, minISR, retry, DLT, producer idempotence

### 학습할 것

- `acks=0`, `acks=1`, `acks=all`
- `min.insync.replicas`
- producer retry와 idempotence
- blocking retry, retry topic, DLT

### 실험

```text
replication.factor=3
min.insync.replicas=2
acks=all
```

- broker 1대를 중단해 ISR=2일 때 전송 성공을 확인한다.
- broker 2대를 중단해 ISR=1일 때 전송 실패를 확인한다.
- 특정 orderId에서 listener 예외를 내어 retry 후 DLT로 가게 한다.

> [!warning] 순서 보장 주의
> retry topic으로 보낸 이벤트는 원래 partition 흐름을 벗어난다. 같은 주문에서 엄격한 순서가 필요하면 non-blocking retry가 적합한지 먼저 검토한다.

완료: producer idempotence와 business-level 중복 제거가 다르다는 점을 설명한다.

## 8. Kafka transaction과 EOS

### 학습할 것

- `transactional.id`, commit, abort
- `read_committed`
- input offset commit과 output publish의 원자성

### 구현과 실험

```text
order-events 수신
  → order-validated-events 발행
  → input offset commit
```

출력 record 발행 직후 프로세스를 종료하고, transaction abort 및 `read_committed` consumer의 관찰 결과를 확인한다.

완료: Kafka EOS가 Kafka → App → Kafka 범위의 보장이라는 점을 설명한다.

## 9. PostgreSQL Transactional Outbox

### 학습할 것

- DB/Kafka dual write 문제
- `orders`와 `outbox_event`를 같은 DB transaction에 저장하는 이유
- polling relay, 상태 전이(`NEW` → `SENT`), 재시도

### 구현

```text
DB transaction
  INSERT orders
  INSERT outbox_event

Relay
  NEW event 조회 → Kafka 전송 → SENT 표시
```

### 필수 장애 실험

Kafka 전송은 성공시키고 `SENT` 갱신 전에 relay를 종료한다. 재시작 후 동일 `eventId`가 재발행되는 것을 확인한다.

완료: Outbox는 dual write 불일치를 줄이지만 exactly-once 전달을 보장하지 않는다는 점을 설명한다.

## 10. Idempotent consumer와 Inbox/Dedup

### 학습할 것

- at-least-once 전달과 중복 side effect
- DB unique constraint 기반 중복 제거
- 조회 후 저장보다 unique constraint가 안전한 이유

### 구현

```text
processed_event
- event_id PK
- processed_at
```

수신 시 `eventId`를 먼저 저장한다. PK 충돌이면 이미 처리한 이벤트로 판단하고 business 처리를 건너뛴다.

### 실험

같은 eventId를 두 번 발행하고 주문 상태 변경이 한 번만 일어나는지 확인한다.

완료: Outbox와 idempotent consumer가 함께 필요한 이유를 설명한다.

## 11. Debezium CDC Outbox

### 학습할 것

- PostgreSQL WAL
- Kafka Connect와 connector offset
- Debezium Outbox Event Router
- polling relay와 CDC의 운영 trade-off

### 실험

직접 만든 polling relay를 유지한 채 Debezium pipeline을 별도 profile로 실행한다.

```text
PostgreSQL WAL → Debezium → Kafka Connect → outbox-event
```

완료: CDC가 애플리케이션 polling 코드를 줄이는 대신 Kafka Connect 운영 요소를 추가한다는 점을 설명한다.

## 12. 선택 확장

- 성능: `linger.ms`, batch, compression, listener concurrency
- 안정성: consumer 처리 시간, `max.poll.interval.ms`, backpressure
- 보안: SASL/SCRAM, ACL, TLS
- 관찰 UI: broker/controller, leader/ISR, lag, outbox, dedup, 장애 타임라인

UI는 기능 구현보다 학습 결과 시각화가 목적이다. 앞선 실험에서 실제로 관찰한 데이터만 표시한다.

## 참고 문서

- [Apache Kafka 4.0 upgrade/KRaft](https://kafka.apache.org/40/getting-started/upgrade/)
- [Spring Kafka retry topic](https://docs.spring.io/spring-kafka/reference/retrytopic/retry-config.html)
- [Spring Kafka exactly once semantics](https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html)
- [Debezium Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)

## 최종 요약

학습의 순서는 Kafka client 사용법, Kafka cluster 신뢰성, Kafka transaction, DB 결합 시 중복 처리 순서다. 각 단계에서 장애를 재현하고 기록하면, Kafka의 보장 범위와 애플리케이션이 책임져야 할 부분을 분명히 구분할 수 있다.
