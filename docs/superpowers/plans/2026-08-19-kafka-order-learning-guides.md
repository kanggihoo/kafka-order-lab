# Kafka 주문 처리 단계별 학습 가이드 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 하나의 커머스 주문 흐름을 step-00부터 step-12까지 점진적으로 고도화하는 Kafka 학습·구현 가이드 13개를 작성한다.

**Architecture:** 모든 가이드는 `docs/steps/`에 독립 파일로 두되, 이전 단계의 실행 상태와 다음 단계로 넘기는 상태를 명시해 순차 실습이 가능하도록 한다. 초기에는 단일 Spring Boot 애플리케이션과 `order-events`·`payment-events`만 사용하고, 인프라와 신뢰성 기술은 필요한 단계에서만 추가한다.

**Tech Stack:** Java 21, Spring Boot 4.1, Gradle, Docker Compose, Apache Kafka KRaft, PostgreSQL, Testcontainers, Debezium (step-11)

**Spec:** `docs/superpowers/specs/2026-08-19-kafka-order-learning-guides-design.md`

## Global Constraints

- 가이드 문서는 `docs/steps/`에 `step-00`부터 `step-12`까지 한 단계당 하나의 Markdown 파일로 만든다.
- 각 문서는 사용자 변화, 목표, 이전 상태, 변경점, 이론, 구현 순서, 실행·검증, 실패 실험, 완료 조건, 다음 단계 연결을 포함한다.
- Java 21, Spring Boot 4.1, Gradle, Docker Compose, PostgreSQL을 기준으로 한다.
- 초기 토픽은 `order-events`, `payment-events`이고 Kafka record key는 `orderId`다.
- 실제 실습 결과 요약 파일은 이 작업에서 만들지 않는다.

---

### Task 1: 기초 이벤트 발행·계약 가이드

**Files:**
- Create: `docs/steps/step-00-single-kafka-order-events.md`
- Create: `docs/steps/step-01-event-contract-json-serialization.md`

- [ ] step-00에서 Docker Compose 단일 KRaft broker, 주문 생성 API, `OrderCreated` 발행·소비, key/partition/offset 관찰을 안내한다.
- [ ] step-01에서 공통 event envelope(`eventId`, `eventType`, `eventVersion`, `occurredAt`, `payload`)와 JSON 직렬화, 호환/비호환 변경 실험을 안내한다.
- [ ] 각 문서에 실제 실행 명령, 예상 관찰값, 다음 단계 연결을 넣는다.

### Task 2: 병렬 소비와 전달 보장 가이드

**Files:**
- Create: `docs/steps/step-02-partition-key-consumer-group.md`
- Create: `docs/steps/step-03-offset-commit-rebalance-lag.md`

- [ ] step-02에서 3 partition, `orderId` key, 결제 consumer group의 partition 할당과 순서 보장을 안내한다.
- [ ] step-03에서 수동 commit, 소비자 강제 종료, rebalance, lag를 통해 at-least-once의 중복 가능성을 재현한다.
- [ ] 두 문서 모두 `order-events`에서 `payment-events`로 이어지는 주문·결제 흐름을 유지한다.

### Task 3: 토픽 저장 전략과 관측 가이드

**Files:**
- Create: `docs/steps/step-04-retention-compaction-order-status.md`
- Create: `docs/steps/step-05-cli-observability-integration-test.md`

- [ ] step-04에서 이력 토픽과 compacted `order-status`를 분리하고 tombstone과 비동기 compaction을 실험한다.
- [ ] step-05에서 Kafka CLI, 애플리케이션 로그, Actuator, Testcontainers 기반 검증과 실험 기록 양식을 안내한다.

### Task 4: 복제와 실패 처리 가이드

**Files:**
- Create: `docs/steps/step-06-kraft-replication.md`
- Create: `docs/steps/step-07-acks-min-isr-retry-dlt.md`

- [ ] step-06에서 3-node KRaft, replication factor 3, controller leader와 partition leader의 차이, broker 중단 실험을 안내한다.
- [ ] step-07에서 `acks=all`, `min.insync.replicas=2`, producer idempotence, blocking retry, DLT 및 순서 손실 주의점을 안내한다.

### Task 5: Kafka와 DB 신뢰성 가이드

**Files:**
- Create: `docs/steps/step-08-kafka-transaction-eos.md`
- Create: `docs/steps/step-09-postgresql-transactional-outbox.md`
- Create: `docs/steps/step-10-idempotent-consumer-inbox.md`

- [ ] step-08에서 입력 소비·출력 발행·offset commit의 Kafka transaction, abort, `read_committed` 검증을 안내한다.
- [ ] step-09에서 `orders`와 `outbox_event`의 단일 DB transaction, polling relay, relay 중단 뒤 중복 발행을 안내한다.
- [ ] step-10에서 `processed_event`의 unique constraint/PK 기반 Inbox로 중복 business side effect를 차단하는 방법을 안내한다.

### Task 6: CDC 및 운영 확장 가이드

**Files:**
- Create: `docs/steps/step-11-debezium-cdc-outbox.md`
- Create: `docs/steps/step-12-performance-security-observability.md`

- [ ] step-11에서 PostgreSQL WAL, Kafka Connect, Debezium Outbox Event Router와 polling relay의 운영 trade-off를 안내한다.
- [ ] step-12에서 성능, 보안, 관측성 중 하나를 실제 측정/실험 기준으로 선택해 확장하도록 안내한다.

### Task 7: 전체 연결성 검토

**Files:**
- Verify: `docs/steps/step-00-*.md` through `docs/steps/step-12-*.md`

- [ ] 모든 문서에 정해진 섹션이 있는지 확인한다.
- [ ] 토픽명, event envelope, `orderId` key, Spring Boot 4.1 기준이 문서 간에 일관적인지 확인한다.
- [ ] 이전 단계에서 아직 도입하지 않은 구성 요소를 요구하지 않는지 확인한다.
- [ ] 실제 결과를 지어낸 summary 문서가 없는지 확인한다.
