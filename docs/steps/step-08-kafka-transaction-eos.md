# Step 08 — Kafka transaction과 EOS

## 사용자가 체감하는 변화

주문 이벤트를 검증해 새 Kafka 이벤트를 발행할 때, 중간에 죽어도 “출력만 보이거나 offset만 이동한” 반쪽 처리를 Kafka 범위에서 피할 수 있다.

## 목표와 완료 조건

- `order-events` 입력을 받아 `order-validated-events` 출력으로 변환한다.
- transactional producer와 `read_committed` consumer를 설정한다.
- 출력 발행 직후 강제 종료/abort에서 uncommitted output이 보이지 않음을 검증한다.
- 보장 범위가 Kafka → 애플리케이션 → Kafka임을 명확히 한다.

## 이전 상태와 이번 변경

3-node cluster와 계약 envelope를 유지한다. DB 쓰기는 하지 않는다. `payment-service`와 별개로 `order-validator` consumer group을 두어 입력과 출력을 분리한다.

## 핵심 이론

Kafka transaction은 transaction에 포함된 output records와 consumer offsets를 함께 commit/abort한다. `read_uncommitted` consumer는 abort된 record도 볼 수 있지만 `read_committed` consumer는 commit된 record만 읽는다. 이것은 PostgreSQL update나 외부 HTTP 호출까지 원자적으로 만들지 않는다.

## 구현 순서

1. producer factory에 instance마다 유일한 `transactionIdPrefix`/`transactional.id` 전략을 설정한다.
2. listener container를 Kafka-aware transaction manager와 연결하고, 입력 offset이 transaction에 포함되게 구성한다.
3. `OrderCreated`를 검증해 `OrderValidated` 또는 `OrderRejected`를 `order-validated-events`에 발행한다.
4. 출력 발행 직후 예외를 던져 transaction abort를 유발하는 테스트 경로를 만든다.
5. `read_committed` observer와 `read_uncommitted` observer를 분리해 관찰한다.
6. 정상 경로에서 output record와 입력 committed offset이 함께 전진했는지 확인한다.

## 실행·검증

abort 실험 뒤 `read_committed` consumer에는 해당 output이 없어야 한다. 입력 record는 재처리될 수 있으므로 정상 재기동 때 하나의 committed output으로 끝나는지 확인한다. abort record가 topic log에 물리적으로 존재할 수 있다는 사실과 소비 가능성은 구분한다.

## 실패 실험

- transaction 밖에서 output을 먼저 보내고 input offset commit 전에 종료한 결과와 비교한다.
- `read_uncommitted` consumer로 abort record가 보일 수 있음을 확인한다.
- DB 상태를 함께 변경하는 코드를 넣어 Kafka EOS가 DB atomicity를 보장하지 않음을 설명한다.

## 다음 단계로 넘기는 상태

Kafka 내부 파이프라인의 원자성은 얻었지만 주문 DB와 Kafka를 동시에 안전하게 쓰는 문제는 남았다. step-09에서 Transactional Outbox를 도입한다.
