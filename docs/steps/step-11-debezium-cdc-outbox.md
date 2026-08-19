# Step 11 — Debezium CDC Outbox

## 사용자가 체감하는 변화

애플리케이션이 outbox table을 직접 polling하지 않아도 PostgreSQL WAL 변경을 Kafka Connect/Debezium이 읽어 Kafka 이벤트로 전달한다. 대신 Connect와 connector offset이라는 운영 대상이 추가된다.

## 목표와 완료 조건

- PostgreSQL logical replication/WAL 설정, Kafka Connect, Debezium connector를 별도 profile로 실행한다.
- Debezium Outbox Event Router로 `outbox_event`를 `order-events`로 라우팅한다.
- connector 재시작과 offset 저장을 관찰한다.
- polling relay와 CDC의 장단점을 실제 구성 기준으로 비교한다.

## 이전 상태와 이번 변경

step-09/10의 `orders`, `outbox_event`, Inbox와 이벤트 계약을 유지한다. application polling relay는 기본 profile에서는 남겨 두되 CDC profile에서는 비활성화해 둘이 동시에 같은 outbox row를 보내지 않게 한다.

## 구현 순서

1. PostgreSQL에 `wal_level=logical`, replication slot/권한을 설정한다. 개발 환경 설정과 운영 권한을 구분한다.
2. Docker Compose에 Kafka Connect와 Debezium connector plugin을 추가한다.
3. outbox table column이 Event Router의 id, aggregate type/id, type, payload, timestamp 매핑과 일치하게 한다.
4. connector 설정에서 event key를 aggregate id(`orderId`)로, topic routing을 `order-events` 또는 event type별 topic으로 명시한다.
5. connector를 등록하고 상태 endpoint에서 RUNNING을 확인한다.
6. 주문을 생성해 PostgreSQL row → WAL → Connect → Kafka record 경로를 순서대로 관찰한다.
7. Connect를 중단/재시작하고 connector offset 때문에 이미 처리한 WAL 변경을 무작정 처음부터 다시 읽지 않는지 확인한다.

## 검증·비교

polling relay는 애플리케이션 코드와 DB polling 부하가 늘지만 구성과 디버깅이 단순하다. CDC는 DB log 기반으로 지연과 polling 코드를 줄일 수 있지만 Kafka Connect, connector lifecycle, replication slot/WAL 보관을 운영해야 한다. 어느 쪽도 consumer Inbox를 제거하지 않는다.

## 실패 실험

- relay와 CDC를 동시에 켜 동일 event가 두 번 발행될 수 있음을 확인한 뒤 즉시 한 경로를 비활성화한다.
- connector 중지 기간에 WAL/slot이 누적되는 관찰값을 기록한다.
- 잘못된 topic routing 또는 key 설정에서 `orderId` 순서 보장이 깨질 수 있음을 확인한다.

## 다음 단계로 넘기는 상태

주문 이벤트의 생성, 복제, 전달, 중복 제거, CDC까지 학습했다. step-12에서는 실제 측정값을 근거로 성능·보안·관측성 중 하나를 선택해 운영 관점으로 확장한다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
