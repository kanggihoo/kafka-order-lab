# Step 02 — Partition, key, consumer group

## 사용자가 체감하는 변화

여러 주문은 병렬로 결제 처리되지만 같은 주문의 이벤트는 순서대로 처리된다. 사용자는 주문이 늘어날수록 consumer 수가 아니라 partition 수가 병렬성 상한이라는 점을 본다.

## 목표와 완료 조건

- `order-events`와 `payment-events`를 3 partition으로 운영한다.
- `orderId` key가 같은 모든 이벤트가 같은 partition으로 가는 것을 보인다.
- 같은 consumer group에서 consumer 1·2·3·4개 실행 시 할당 변화를 기록한다.

## 이전 상태와 이번 변경

step-01의 envelope를 그대로 쓴다. 주문 생성 producer 외에 `payment-service` consumer group을 추가한다. 이 consumer는 `OrderCreated`를 받아 가짜 결제 요청 이벤트를 `payment-events`에 발행한다.

## 핵심 이론

consumer group에서는 하나의 partition을 동시에 둘 이상의 consumer가 읽지 않는다. partition 3개에서 consumer가 4개면 한 consumer는 idle이다. 같은 key의 순서 보장은 producer가 같은 topic에 같은 key로 보내고, consumer가 partition 내부 처리를 뒤섞지 않을 때 성립한다.

## 구현 순서

1. 기존 토픽을 삭제·재생성하거나 새 환경에서 두 토픽을 3 partition으로 만든다. partition 변경은 기존 key 분포를 바꿀 수 있으므로 실습 환경에서만 수행한다.
2. `payment-service`의 listener group id를 고정한다.
3. `OrderCreated` 수신 시 `PaymentRequested`를 같은 `orderId` key로 `payment-events`에 발행한다.
4. 가짜 결제 observer를 두어 `PaymentRequested`의 metadata를 기록한다.
5. 애플리케이션 인스턴스를 1, 2, 3, 4개로 실행하며 `Assigned partitions` 로그를 비교한다.
6. `1001`에 대해 `OrderCreated`, 후속 주문 이벤트를 보내 partition 번호가 같은지 확인한다. `1002`~`1010`은 여러 partition에 분산되는지 확인한다.

## 실행·검증

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic order-events
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group payment-service --describe
```

처리량을 올리려면 consumer만 늘리지 말고 partition 수, key 분포, 처리 시간을 함께 봐야 한다. 특정 `orderId`에 트래픽이 몰리면 그 key가 있는 partition이 hot partition이 된다.

## 실패 실험

- producer에서 key를 `null`로 보내 같은 주문 이벤트가 여러 partition으로 갈 수 있음을 확인한다.
- listener concurrency를 partition 수보다 크게 설정해도 추가 병렬성이 생기지 않음을 확인한다.

## 다음 단계로 넘기는 상태

결제 흐름이 병렬화됐지만 consumer가 언제 offset을 commit하는지는 아직 통제하지 않는다. step-03에서 수동 commit과 강제 종료로 중복·유실 위험을 관찰한다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
