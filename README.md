# Kafka 주문 처리 학습 랩 (kafka-order-lab)

> **Kafka 기반 주문 처리 파이프라인의 신뢰성, 장애 복구 및 중복 처리 패턴을 단계별로 검증하는 실습 프로젝트**

`orderId`를 Kafka record key로 사용하는 주문 처리 도메인을 바탕으로, Kafka의 기본 동작부터 클러스터 장애, 메시지 중복, Transactional Outbox 및 Idempotent Consumer(Inbox) 패턴까지 단계적으로 검증하고 실험 결과를 기록합니다.

---

## 🎯 학습 원칙

> [!IMPORTANT]
> 각 단계는 단순한 기능 구현이 아니라 **실패 및 장애 상황을 직접 재현하고, 기대한 보장(Guarantees)이 실제로 지켜지는지 확인**하는 것으로 완료합니다. 모든 실험 결과는 [`docs/experiments/`](docs/experiments/)에 기록됩니다.

### 핵심 탐구 과제 (3대 핵심 질문)
1. **Broker 장애 시 전송 보장**: Broker가 다운되었을 때 `acks=all`과 `min.insync.replicas` 조건에서 어떤 전송이 성공하거나 실패하는가?
2. **Outbox Relay 중복 발행**: Outbox relay가 Kafka 전송 성공 후 DB 상태 갱신 전에 비정상 종료되면 왜 중복 발행이 발생하는가?
3. **Consumer 중복 처리 방지**: Consumer가 DB 비즈니스 로직 처리 후 offset commit 전에 비정상 종료되면 왜 Idempotent Consumer(Inbox)가 필수적인가?

---

## 🏗️ 도메인 및 이벤트 흐름

### 이벤트 파이프라인
```text
POST /orders
  │
  ▼
OrderCreated
  │
  ▼
PaymentRequested
  │
  ├──► PaymentApproved ──► InventoryReserved ──► OrderCompleted
  │
  └──► PaymentFailed   ───────────────────────► OrderCancelled
```

- **Kafka Key**: `orderId` (동일 주문에 속한 이벤트는 동일 Partition으로 전송되어 **순서 보장**)
- **공통 이벤트 구조**:
```json
{
  "eventId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-08-19T10:00:00Z",
  "payload": {
    "orderId": 1001,
    "userId": "user-123",
    "amount": 25000
  }
}
```

### 토픽 구성 및 정리 정책

| Topic | Key | 역할 및 목적 | Cleanup Policy |
|---|---|---|---|
| `order-events` | `orderId` | 주문 생명주기 이벤트 이력 | `delete` (retention) |
| `payment-events` | `orderId` | 결제 결과 이벤트 이력 | `delete` (retention) |
| `order-status` | `orderId` | 주문별 최신 상태 보관 | `compact` |
| `order-events-retry-*` | 원본 key | 재시도 큐 | `delete` (retention) |
| `order-events-dlt` | 원본 key | 최종 처리 실패 메시지 (Dead Letter Topic) | `delete` (retention) |
| `outbox-event` | `eventId` | Outbox/CDC 발행 결과 | `delete` (retention) |

---

## 🗺️ 학습 로드맵

```text
[최우선 완주 코스]
Step 00 (기본 연동) ─► Step 03 (Offset/Lag) ─► Step 05 (관찰/테스트) ─► Step 06 (KRaft 복제) ─► Step 07 (acks/Retry/DLT) ─► Step 09 (Transactional Outbox) ─► Step 10 (Inbox/Dedup)
```

| 주차 | 단계 | 주제 | 최소 완료 결과 |
|:---:|:---:|---|---|
| **1주차** | **Step 00** | 단일 Kafka와 Spring Boot 연결 | 주문 이벤트 발행·수신 및 partition/offset 출력 확인 |
| | **Step 01** | 메시지 계약과 JSON 직렬화 | `eventId`, `eventVersion` 기반 역직렬화 및 스키마 진화 검증 |
| **2주차** | **Step 02** | Partition, Key, Consumer Group | Consumer 수에 따른 Partition 할당 및 리밸런싱 재현 |
| **3주차** | **Step 03** | Offset, Commit, Rebalance, Lag | 장애 재시작 시 중복 및 유실 가능성 재현 |
| **4주차** | **Step 04** | Retention, Log Compaction | 이벤트 이력 Topic(`delete`)과 최신 상태 Topic(`compact`) 분리 검증 |
| | **Step 05** | CLI, 지표, 통합 테스트 | lag, leader, ISR 확인 및 실험 템플릿 작성 |
| **5주차** | **Step 06** | 3-node KRaft, Replication | Broker/Controller 장애 시 Leader 선출 관찰 |
| **6주차** | **Step 07** | acks, minISR, Retry, DLT | ISR 감소 시 전송 실패 및 Non-blocking Retry/DLT 처리 |
| **7주차** | **Step 08** | Kafka Transaction & EOS | Kafka Transaction 기반 원자적 처리 및 `read_committed` 확인 |
| **8주차** | **Step 09** | PostgreSQL Transactional Outbox | DB 변경과 이벤트 발행 의도를 하나의 DB 트랜잭션으로 저장 |
| **9주차** | **Step 10** | Idempotent Consumer & Inbox | DB Unique 제약 조건을 활용한 비즈니스 멱등성 보장 |
| **10주차**| **Step 11** | Debezium CDC Outbox | Polling Relay와 PostgreSQL WAL 기반 CDC 비교 검증 |
| **선택** | **Step 12** | 성능, 보안, 관찰 UI | Batch/Linger 튜닝, ACL/SASL 보안 및 대시보드 구축 |

---

## 🛠️ 기술 스택

- **언어 및 프레임워크**: Java 21, Spring Boot 4.1.x, Spring Kafka, Spring Data JPA / Web
- **메시지 브로커**: Apache Kafka 4.1.x (KRaft 모드)
- **데이터베이스**: PostgreSQL
- **인프라 & 도구**: Docker Compose, Testcontainers, Debezium CDC
- **모니터링 & 지표**: Spring Boot Actuator, Micrometer, Kafka CLI

---

## 🚀 빠른 시작 (Getting Started)

### 1. Kafka 클러스터 실행 (KRaft)
```bash
docker compose up -d
```

### 2. Spring Boot 애플리케이션 빌드 및 실행
```bash
./gradlew bootRun
```

### 3. 주문 생성 이벤트 테스트
```bash
# 주문 이벤트 발행 (orderId=1001)
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": 1001,
    "userId": "user-01",
    "amount": 25000
  }'
```

### 4. CLI를 통한 토픽 및 컨슈머 그룹 관찰
```bash
# 토픽 목록 및 상세 정보 확인
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic order-events

# 실시간 메시지 수신 확인
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-events --from-beginning --property print.key=true

# 컨슈머 그룹 상태 및 Lag 확인
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-service-group
```

---

## 📚 상세 문서

- 📋 [Kafka 주문 처리 학습 로드맵](docs/Kafka%20주문%20처리%20학습%20로드맵.md)
- 📏 [Java 구현 최소 지침서](docs/Java%20구현%20최소%20지침서.md)
- 📖 [Kafka 주문 처리 학습 상세 가이드](docs/Kafka%20주문%20처리%20학습%20상세%20가이드.md)
- 📝 [단계별 실습 가이드 (Step 00 ~ 12)](docs/steps/)
- 🔬 [장애 실험 기록 (Experiments)](docs/experiments/)
