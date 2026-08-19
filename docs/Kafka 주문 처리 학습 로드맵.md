---
title: Kafka 주문 처리 학습 로드맵
tags:
  - kafka
  - spring-boot
  - learning
status: planned
---

# Kafka 주문 처리 학습 로드맵

`orderId`를 Kafka record key로 쓰는 주문 처리 도메인 하나를 유지하면서, Kafka의 기본 동작부터 장애·중복·Outbox까지 단계적으로 검증한다.

> [!important] 학습 원칙
> 각 단계는 기능 구현이 아니라 **실패 상황을 만들고, 기대한 보장이 실제로 지켜지는지 확인**하는 것으로 완료한다. 실험 결과는 `docs/experiments/`에 남긴다.

## 도메인과 이벤트 흐름

```text
POST /orders
  → OrderCreated
  → PaymentRequested
  → PaymentApproved | PaymentFailed
  → InventoryReserved
  → OrderCompleted | OrderCancelled
```

```text
Kafka key = orderId
```

같은 주문의 이벤트는 동일 partition으로 보내 순서를 유지한다. 이벤트 payload에는 `eventId`, `eventType`, `eventVersion`, `occurredAt`, `payload`를 공통으로 둔다.

## 주차별 로드맵

| 주차 | 단계 | 주제 | 최소 완료 결과 |
|---:|---:|---|---|
| 1 | 0 | 단일 Kafka와 Spring Boot 연결 | 주문 이벤트를 발행·수신하고 partition/offset을 출력한다. |
| 1 | 1 | 메시지 계약과 JSON 직렬화 | eventId·version이 있는 이벤트를 역직렬화한다. |
| 2 | 2 | Partition, key, consumer group | consumer 수에 따른 partition 할당을 재현한다. |
| 3 | 3 | Offset, commit, rebalance, lag | 재시작 때 중복/유실 가능성을 재현한다. |
| 4 | 4 | Retention, compaction | 이벤트 이력 topic과 최신 상태 topic을 분리한다. |
| 4 | 5 | CLI, 지표, 통합 테스트 | lag·leader·ISR을 확인하고 실험 결과를 기록한다. |
| 5 | 6 | 3-node KRaft, replication | broker/controller 장애와 leader 선출을 관찰한다. |
| 6 | 7 | acks, minISR, retry, DLT | ISR 감소 시 전송 실패와 retry/DLT를 재현한다. |
| 7 | 8 | Kafka transaction/EOS | Kafka → App → Kafka의 abort와 read_committed를 확인한다. |
| 8 | 9 | PostgreSQL Transactional Outbox | DB 변경과 발행 의도를 한 transaction으로 저장한다. |
| 9 | 10 | Idempotent consumer/Inbox | 중복 이벤트의 business side effect를 한 번으로 제한한다. |
| 10 | 11 | Debezium CDC Outbox | polling relay와 WAL 기반 CDC를 비교한다. |
| 선택 | 12 | 성능, 보안, 관찰 UI | 측정 가능한 병목 또는 보안 요구에 맞춰 확장한다. |

## 최우선 완주 범위

시간이 부족하면 다음 단계를 우선 완주한다.

```text
0 → 3 → 5 → 6 → 7 → 9 → 10
```

핵심 질문은 세 가지다.

1. broker가 죽으면 `acks=all`과 `min.insync.replicas` 조건에서 어떤 전송이 성공하거나 실패하는가?
2. Outbox relay가 Kafka 전송 성공 뒤 죽으면 왜 중복 발행되는가?
3. consumer가 DB 처리 뒤 offset commit 전에 죽으면 왜 중복 제거가 필요한가?

## 프로젝트 진행 규칙

- 처음에는 Spring Boot 애플리케이션 하나만 유지한다. 학습을 위해 마이크로서비스로 분리하지 않는다.
- Kafka UI는 초기에 만들지 않는다. CLI, 로그, Actuator로 먼저 관찰한 뒤 마지막에 캡스톤으로 만든다.
- 각 장애 실험은 설정, 행동, 예상, 실제 결과, 결론을 Markdown으로 기록한다.
- DB, Debezium, Schema Registry 같은 구성 요소는 해당 단계 전에는 추가하지 않는다.

## 상세 가이드

- [[Kafka 주문 처리 학습 상세 가이드]]

## 최종 요약

이 로드맵은 Kafka API 사용법보다 장애 시 전달 보장과 중복 처리의 한계를 확인하는 데 초점을 둔다. Kafka 내부 보장부터 DB와 결합한 실제 백엔드의 보장 한계까지, 하나의 주문 도메인으로 연결해 학습한다.
