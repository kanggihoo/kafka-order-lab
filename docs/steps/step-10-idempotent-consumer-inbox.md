# Step 10 — Idempotent consumer와 Inbox/Dedup

## 사용자가 체감하는 변화

relay 재시작이나 consumer 재처리로 같은 결제 이벤트가 여러 번 도착해도 주문 상태 변경, 결제 승인 기록 같은 업무 효과는 한 번만 일어난다.

## 목표와 완료 조건

- `processed_event` Inbox 테이블을 둔다.
- `event_id` PK/unique constraint로 중복을 원자적으로 판별한다.
- 같은 event를 두 번 발행해도 주문 상태 변경이 한 번만 발생함을 검증한다.
- “먼저 조회하고 없으면 insert”가 경쟁 조건인 이유를 설명한다.

## 이전 상태와 이번 변경

step-09의 PostgreSQL과 Outbox relay를 유지한다. `PaymentApproved` 또는 `PaymentFailed`를 소비해 `orders.status`를 바꾸는 consumer를 구현한다. Kafka offset commit은 성공적인 DB transaction 뒤에만 진행한다.

## 데이터 모델

```sql
create table processed_event (
  event_id uuid primary key,
  consumer_name varchar(100) not null,
  processed_at timestamptz not null
);
```

consumer별로 같은 event를 독립 처리해야 한다면 PK를 `(consumer_name, event_id)`로 바꾼다. 어떤 의미를 쓸지 문서와 코드에서 하나로 고정한다.

## 구현 순서

1. 결제 결과 consumer의 DB transaction 안에서 먼저 `processed_event` insert를 시도한다.
2. insert가 성공한 경우에만 `orders.status`를 `PAID` 또는 `CANCELLED`로 바꾸고 필요한 후속 outbox event를 추가한다.
3. PK 충돌이면 이미 처리한 event로 판단해 업무 update를 건너뛴다. 오류가 아니라 정상 중복 경로로 로그를 남긴다.
4. DB transaction이 commit된 뒤 Kafka offset을 ack한다. commit 전 consumer 종료는 재전달을 만들지만 Inbox가 다시 업무 처리하는 것을 막는다.
5. 동시 consumer가 같은 event를 받는 테스트 또는 두 번 발행 테스트를 작성한다.

## 실행·검증

동일한 `eventId`와 동일한 payload를 두 번 발행한다. `processed_event`에는 1 row, 주문 상태 전이 로그에는 1회만 업무 처리가 남아야 한다. 두 번째 record의 Kafka offset은 계속 commit되어야 하므로 listener가 예외를 던져 무한 retry하지 않게 한다.

## 실패 실험

- `select`로 존재 여부를 확인한 뒤 insert하는 구현을 만들어 동시 실행 시 둘 다 “없음”을 볼 수 있음을 관찰한다.
- Inbox insert 뒤 주문 update 전에 예외를 낸다면 같은 transaction으로 rollback되어야 한다.
- Inbox를 Kafka transaction의 대체물로 오해하지 않는다. 이것은 DB 업무 부작용의 멱등성을 제공한다.

## 다음 단계로 넘기는 상태

Outbox + Inbox로 polling relay 기반의 at-least-once 흐름을 안전하게 만들었다. step-11에서 application polling을 Debezium CDC로 교체해 WAL 기반 전달과 운영 trade-off를 비교한다.
