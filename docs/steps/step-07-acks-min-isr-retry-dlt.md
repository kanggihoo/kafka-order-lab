# Step 07 — acks, min ISR, retry, DLT, producer idempotence

## 사용자가 체감하는 변화

주문을 접수했다는 응답이 어느 정도의 복제 내구성을 뜻하는지 알 수 있고, 일시 처리 실패와 최종 실패 주문을 분리해 추적할 수 있다.

## 목표와 완료 조건

- RF=3, `min.insync.replicas=2`, producer `acks=all`을 적용한다.
- broker 1개 중단 시 전송 성공, 2개 중단으로 ISR 1일 때 전송 실패를 재현한다.
- listener 예외를 blocking retry 후 DLT로 보낸다.
- producer idempotence와 business-level dedup이 다름을 설명한다.

## 이전 상태와 이번 변경

step-06의 3-node cluster를 사용한다. `payment-service`에 예외를 의도적으로 내는 가짜 결제 규칙(예: `amount=7777`)을 추가한다. Inbox는 step-10 전까지 구현하지 않는다.

## 구현 순서

1. 두 이력 토픽에 RF=3과 `min.insync.replicas=2`를 적용한다.
2. producer에 `acks=all`, idempotence 활성화, 적절한 delivery timeout을 설정한다. idempotence를 켜도 애플리케이션 재시작·새 eventId·업무 중복을 해결하지 못함을 주석으로 남긴다.
3. leader/ISR를 확인한 뒤 broker 하나를 멈추고 주문 발행이 성공하는지 본다.
4. 두 번째 broker를 멈춰 ISR가 1인 상태에서 발행이 실패하는지 확인한다. 실패를 억지로 성공 처리하지 않는다.
5. listener에 `DefaultErrorHandler`와 고정 backoff를 설정해 같은 partition에서 blocking retry한다.
6. 정해진 횟수 뒤 `order-events-dlt` 또는 `payment-events-dlt`에 원본 key, eventId, 예외 정보가 보존되게 한다.
7. DLT consumer는 자동 재처리하지 않고 관찰/수동 복구용으로 둔다.

## 실행·검증

DLT record에는 원본 topic, partition, offset, exception header가 있어야 한다. retry topic 방식은 원래 partition 순서를 깨뜨릴 수 있으므로, 이 단계에서는 순서를 지키는 blocking retry를 먼저 학습한다.

```bash
kafka-consumer-groups.sh --bootstrap-server kafka-1:9092 --group payment-service --describe
kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic payment-events-dlt --from-beginning --property print.headers=true --property print.key=true
```

## 실패 실험

- ISR=2와 ISR=1에서 동일 주문 발행 결과를 비교한다.
- 지정 주문에서 listener 예외를 내어 retry 횟수, 간격, 최종 DLT를 기록한다.
- non-blocking retry topic으로 바꿨을 때 같은 `orderId`의 후속 이벤트가 앞설 수 있는지 별도 실험으로 확인한다.

## 다음 단계로 넘기는 상태

producer와 consumer의 단일 record 실패를 다뤘다. step-08에서는 Kafka에서 읽은 입력과 Kafka로 낸 출력을 offset commit까지 원자적으로 묶는다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
