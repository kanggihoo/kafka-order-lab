---
title: Kafka 주문 처리 단계별 학습·구현 가이드 설계
status: proposed
---

# Kafka 주문 처리 단계별 학습·구현 가이드 설계

## 목적

Kafka의 전달 보장, 순서, 장애 복구, DB 결합 문제를 하나의 커머스 주문 도메인에서 점진적으로 학습한다. 학습자는 이전 단계의 실행 가능한 코드를 유지한 채 다음 단계의 변경만 추가한다. 각 단계는 실습뿐 아니라 의도적인 장애·중복·지연 실험과 관찰을 요구한다.

## 산출물과 위치

`docs/steps/` 아래에 `step-00`부터 `step-12`까지 단계별 Markdown 가이드 13개를 둔다. 파일명은 단계 번호와 핵심 주제를 포함한다.

각 가이드는 다음 순서를 고정한다.

1. 이번 단계에서 사용자가 체감하는 변화
2. 학습 목표와 완료 조건
3. 이전 단계에서 이어받는 코드/인프라 상태
4. 도메인·이벤트·토픽의 변경점
5. 알아야 할 이론
6. 구현 작업을 작은 단위로 나눈 순서
7. 실행과 검증 절차
8. 의도적 실패 실험과 기록할 관찰값
9. 다음 단계로 넘기는 상태

실제 구현이 끝난 뒤에만 같은 디렉터리에 `step-XX-summary.md`를 추가한다. 이 설계 작업에서는 summary 파일을 만들지 않는다.

## 공통 도메인과 초기 흐름

도메인은 단일 Spring Boot 4.1 애플리케이션 안의 커머스 주문 처리다. 처음에는 마이크로서비스로 분리하지 않는다.

```text
POST /orders
  -> OrderCreated (order-events)
  -> PaymentRequested (payment-events)
  -> PaymentApproved | PaymentFailed (payment-events)
  -> InventoryReserved
  -> OrderCompleted | OrderCancelled (order-events)
```

초기 토픽은 `order-events`와 `payment-events`다. 두 토픽 모두 key는 `orderId`로 사용한다. `order-status`는 step-04에서 compaction 학습을 위해 추가한다. retry/DLT, outbox, 검증용 토픽은 필요한 단계에서만 추가한다.

결제는 외부 PG 대신 결정적 가짜 결제 처리기를 사용한다. 예를 들어 요청 값 또는 주문 금액 규칙으로 승인과 실패를 재현해 실험 결과가 반복 가능해야 한다.

## 단계 연결

| 단계 | 새로 고도화하는 능력 | 전 단계와의 연결 |
|---|---|---|
| 00 | Spring Boot와 단일 Kafka, 발행·소비 | 주문 생성 후 `OrderCreated`를 관찰한다. |
| 01 | JSON 이벤트 계약과 호환성 | 00의 단순 메시지를 버전 있는 공통 envelope로 바꾼다. |
| 02 | key, partition, consumer group | 주문별 순서와 결제 소비자의 병렬 처리를 확인한다. |
| 03 | 수동 commit, rebalance, lag | 02의 소비 흐름에서 중복·유실 위험을 장애로 재현한다. |
| 04 | retention, compaction | 이력 이벤트와 최신 주문 상태를 분리한다. |
| 05 | CLI, 로그, Actuator, 통합 테스트 | 00~04의 관찰·검증을 반복 가능한 절차로 만든다. |
| 06 | 3-node KRaft와 복제 | 단일 broker 가정을 제거하고 leader/ISR 변화를 본다. |
| 07 | acks, min ISR, retry, DLT | 06의 복제 환경에서 전송 실패와 처리 실패를 다룬다. |
| 08 | Kafka transaction/EOS | Kafka 입력-출력과 offset을 하나의 원자적 처리로 묶는다. |
| 09 | PostgreSQL Transactional Outbox | 주문 DB 변경과 Kafka 발행의 dual write 문제를 해결한다. |
| 10 | Idempotent consumer/Inbox | 09에서 허용되는 중복 발행이 업무 부작용을 만들지 않게 한다. |
| 11 | Debezium CDC Outbox | polling relay를 WAL 기반 파이프라인과 비교한다. |
| 12 | 성능, 보안, 관측성 선택 확장 | 앞선 실험에서 확인한 병목 또는 운영 요구를 한 가지 확장한다. |

## 구현 및 검증 원칙

- Java 21, Spring Boot 4.1, Gradle, Docker Compose, PostgreSQL을 기준 환경으로 쓴다.
- 도입하지 않은 구성 요소(DB, Debezium, 다중 broker, Schema Registry)는 그 이전 단계에서 언급만 하고 설치·구현하지 않는다.
- 각 단계는 명령어, 기대 결과, 실제로 관찰할 로그/CLI 지표를 구분해서 제공한다.
- 장애 실험에는 설정, 행동, 예상, 실제 결과, 결론을 `docs/experiments/`에 남기도록 안내한다.
- Kafka producer idempotence, Kafka EOS, Outbox, Inbox의 보장 범위를 서로 혼동하지 않도록 매 단계 명시한다.

## 제외 범위

- 실제 결제대행사, 실제 재고 시스템 연동
- 초반부터의 마이크로서비스 분리
- UI를 통한 모니터링 구현
- Schema Registry의 실제 도입

UI와 Schema Registry는 step-12 확장 후보로만 다룬다.
