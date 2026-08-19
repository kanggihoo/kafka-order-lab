# Step 09 — PostgreSQL Transactional Outbox

## 사용자가 체감하는 변화

주문 생성 요청이 DB에는 저장됐는데 Kafka 이벤트가 사라지거나, 이벤트는 나갔는데 주문이 없는 dual write 불일치를 피한다. 대신 relay 장애 뒤 같은 이벤트가 다시 나갈 수 있음을 받아들인다.

## 목표와 완료 조건

- PostgreSQL에 `orders`, `outbox_event` 테이블을 추가한다.
- 주문 row와 outbox row를 같은 DB transaction으로 저장한다.
- polling relay가 `NEW` outbox event를 Kafka로 보내고 `SENT`로 바꾼다.
- Kafka 발행 성공 후 `SENT` 갱신 전 relay 종료 시 중복 발행을 재현한다.

## 이전 상태와 이번 변경

step-08의 Kafka transaction은 Kafka 내부 실습으로 남긴다. 주문 생성 API의 source of truth를 PostgreSQL로 옮기고, HTTP 요청 스레드에서 직접 Kafka publish하던 코드를 outbox insert로 교체한다. Debezium은 step-11 전까지 도입하지 않는다.

## 데이터 모델

```sql
create table orders (
  order_id varchar(64) primary key,
  amount numeric(19,2) not null,
  status varchar(32) not null,
  created_at timestamptz not null
);

create table outbox_event (
  event_id uuid primary key,
  aggregate_type varchar(64) not null,
  aggregate_id varchar(64) not null,
  event_type varchar(128) not null,
  event_version integer not null,
  payload jsonb not null,
  status varchar(16) not null,
  created_at timestamptz not null,
  sent_at timestamptz null
);
```

`aggregate_id`는 `orderId`이며 Kafka key가 된다. `event_id`는 계약 envelope의 `eventId`와 같다.

## 구현 순서

1. Docker Compose에 PostgreSQL과 애플리케이션 datasource를 추가한다.
2. `POST /orders`에서 order insert와 `OrderCreated` outbox insert를 하나의 `@Transactional` service method로 처리한다.
3. commit 뒤 주기적으로 `NEW` row를 조회하는 relay를 만든다. 여러 relay가 가능해지는 미래를 위해 row lock/claim 전략(`FOR UPDATE SKIP LOCKED` 등)을 명시한다.
4. relay가 outbox payload를 envelope로 복원해 `order-events`에 key=`aggregate_id`로 보낸다.
5. Kafka send 성공 뒤 같은 row를 `SENT`, `sent_at`으로 갱신한다.
6. 시작 시 `NEW`를 다시 처리하도록 해 장애 복구를 확인한다.

## 실행·검증

주문 요청 뒤 DB에서 order와 NEW outbox row가 동시에 보이는지 확인한다. relay가 돌면 order-events에 발행되고 row는 SENT가 된다. PostgreSQL transaction commit 전에 프로세스를 중단하면 둘 다 남지 않아야 한다.

```sql
select order_id, status from orders;
select event_id, aggregate_id, event_type, status, sent_at from outbox_event order by created_at;
```

## 실패 실험

- order insert 직후 outbox insert 전에 예외를 내고 transaction rollback을 확인한다.
- Kafka send 성공 callback 뒤 `SENT` update 전에 relay를 강제 종료한다.
- 재시작 후 같은 `eventId`가 Kafka에 다시 발행되는지 확인한다.

이 중복은 Outbox의 결함이 아니라 DB/Kafka 분산 transaction 없이 복구 가능성을 확보하는 대가다.

## 다음 단계로 넘기는 상태

이제 producer 쪽 중복 발행은 의도적으로 허용된다. step-10에서 consumer가 `eventId`를 DB unique constraint로 한 번만 업무 처리하게 만든다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
