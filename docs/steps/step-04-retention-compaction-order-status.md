# Step 04 — Retention과 compaction으로 이력과 상태 분리

## 사용자가 체감하는 변화

주문 이력은 그대로 보관하면서도, 주문 화면이나 조회 기능에 필요한 최신 상태는 별도 토픽에서 빠르게 재구성할 수 있다.

## 목표와 완료 조건

- `order-events`는 delete retention 기반 이력 토픽으로 유지한다.
- `order-status` compacted 토픽을 추가한다.
- 같은 key의 CREATED → PAID → COMPLETED 상태를 발행하고 최신 상태 중심으로 읽는다.
- tombstone과 compaction이 즉시 실행되지 않는다는 점을 관찰한다.

## 이전 상태와 이번 변경

`order-events`와 `payment-events`는 3 partition, 계약 envelope, manual commit 상태다. 새 `order-status` 토픽만 추가한다. 이것은 원본 이력을 대체하지 않는다.

## 핵심 이론

`cleanup.policy=delete`는 시간/용량 정책에 따라 오래된 record를 삭제한다. `cleanup.policy=compact`는 key마다 최신 record를 남기는 방향으로 정리한다. compaction은 백그라운드 작업이며 “마지막 값만 즉시 보이는 queue”가 아니다. null value의 tombstone은 해당 key 삭제 의도를 나타낸다.

## 구현 순서

1. `order-status`를 `cleanup.policy=compact`로 만든다. 실습을 위해 segment/retention 관련 설정은 작은 값으로 두되 production 값으로 일반화하지 않는다.
2. 주문 흐름의 각 상태 변경 시 `OrderStatusChanged`를 key=`orderId`로 발행한다.
3. `1001`에 CREATED, PAID, COMPLETED를 순서대로 보낸다.
4. 새 consumer가 처음부터 읽어 in-memory projection을 만들고, 최종 상태가 COMPLETED임을 확인한다.
5. `value=null` tombstone을 보낸 뒤 projection에서 해당 주문을 제거한다.
6. topic을 즉시 읽었을 때 이전 값도 남을 수 있음을 기록하고, broker compaction 뒤 결과를 재확인한다.

## 실행·검증

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic order-status
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic order-status --from-beginning --property print.key=true
```

이력 감사나 재처리는 `order-events`를 사용하고, 현재 상태 화면/캐시는 `order-status` projection을 사용한다. compacted topic만으로 모든 과거 전이를 복원하려고 하지 않는다.

## 실패 실험

- compaction 직후에 이전 상태 record가 남아 있는 모습을 기록한다.
- key 없이 상태 이벤트를 보내 compaction의 의미가 사라짐을 확인한다.
- tombstone을 일반 JSON `null` 문자열과 혼동하지 않도록 실제 Kafka null value를 보낸다.

## 다음 단계로 넘기는 상태

토픽과 consumer 동작을 눈으로 확인할 필요가 커졌다. step-05에서 CLI, Actuator, 로그, Testcontainers로 지금까지의 검증을 반복 가능하게 만든다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
