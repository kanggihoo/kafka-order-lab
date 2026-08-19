# Step 05 — CLI, 관측성, 통합 테스트

## 사용자가 체감하는 변화

문제가 생겼을 때 “메시지를 보냈다”가 아니라 어느 partition에서 consumer가 어디까지 읽었고 얼마나 밀렸는지, 코드와 CLI로 재현 가능하게 확인한다.

## 목표와 완료 조건

- topic의 partition/leader/ISR, consumer group offset/lag를 CLI로 읽는다.
- 발행·소비 로그에 `eventId`, `orderId`, topic, partition, offset을 남긴다.
- Testcontainers Kafka 통합 테스트로 주문 이벤트 발행/소비를 검증한다.
- 주요 장애 실험을 `docs/experiments/`에 기록한다.

## 이전 상태와 이번 변경

step-00~04의 주문·결제 흐름과 세 토픽을 유지한다. 사용자 기능을 추가하기보다 관찰과 검증 도구를 추가한다. 아직 다중 broker는 step-06 전까지 사용하지 않는다.

## 구현 순서

1. Actuator에서 health, metrics를 노출하고 운영용 endpoint는 로컬 개발에만 열도록 profile을 분리한다.
2. producer callback과 모든 listener에 MDC 또는 구조화 로그 필드를 넣는다.
3. 아래 CLI 명령을 `docs/experiments/` 템플릿에 함께 적는다.

```bash
kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic order-events
kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group payment-service --describe
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic payment-events --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

4. Testcontainers Kafka를 띄우는 `@SpringBootTest`를 만든다.
5. 주문 API 또는 producer를 호출하고 `order-events`에서 key와 envelope를 소비하는 테스트를 작성한다.
6. broker URL을 테스트 컨테이너의 bootstrap server로 동적 주입한다.

## 검증 기준

- `order-events`는 partition 수와 leader 정보를 보여야 한다. 단일 broker에서는 replica/ISR의 한계를 함께 기록한다.
- group describe의 CURRENT-OFFSET, LOG-END-OFFSET, LAG를 partition별로 해석한다.
- 테스트는 임의 sleep 대신 consumer poll 또는 latch의 제한 시간을 사용한다.
- 실패 시 이벤트 내용뿐 아니라 consumer group, partition, offset을 출력한다.

## 실패 실험

- consumer를 멈춘 뒤 10개 주문을 보내고 lag가 10 근처로 증가한 뒤 재시작 시 감소하는지 본다.
- 잘못된 bootstrap server profile로 실행해 health와 producer 오류의 차이를 본다.
- topic을 잘못 입력해 listener가 받지 못하는 경우 topic describe로 원인을 찾는다.

## 다음 단계로 넘기는 상태

관측 절차가 준비됐다. step-06에서 broker를 3개로 늘려 controller, leader, replica, ISR의 변화를 이 도구로 확인한다.
