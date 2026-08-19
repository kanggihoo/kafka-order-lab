# Step 06 — 3-node KRaft와 replication

## 사용자가 체감하는 변화

broker 하나가 멈춰도 주문 이벤트가 계속 처리될 수 있는 조건과, 그 조건이 충족되지 않으면 왜 실패해야 하는지를 직접 확인한다.

## 목표와 완료 조건

- 3-node KRaft cluster를 Docker Compose로 실행한다.
- topic을 partition 3, replication factor 3으로 만든다.
- broker 중단 후 controller leader와 partition leader 변화를 관찰한다.
- ISR이 무엇이며 controller leader와 partition leader가 다른 역할임을 설명한다.

## 이전 상태와 이번 변경

애플리케이션 코드의 이벤트 계약과 토픽 명칭은 유지한다. 단일 broker Docker Compose를 controller quorum을 포함한 3 broker 구성으로 교체한다. DB와 retry는 아직 도입하지 않는다.

## 핵심 이론

KRaft controller quorum은 cluster metadata와 controller 선출을 담당한다. partition leader는 특정 partition의 read/write를 담당하고 follower replica는 leader를 복제한다. ISR(In-Sync Replicas)은 leader를 충분히 따라잡은 replica 집합이다. controller leader가 곧 모든 partition leader라는 뜻은 아니다.

## 구현 순서

1. 각 broker에 고유 node id, listener, advertised listener, controller quorum voters를 설정한다.
2. 모든 broker가 controller와 broker 역할을 함께 가지는 개발용 combined mode임을 문서에 명시한다.
3. `order-events`, `payment-events`, `order-status`를 RF=3으로 만든다. compacted 토픽의 정책도 유지한다.
4. `kafka-topics.sh --describe`로 partition별 leader, replicas, ISR을 기록한다.
5. 한 broker를 중단하고 leader 변경과 ISR 변화를 관찰한다.
6. 다시 기동해 replica가 ISR로 돌아오는 과정을 관찰한다.

## 실행·검증

```bash
docker compose up -d
docker compose exec kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic order-events
docker compose stop kafka-2
docker compose exec kafka-1 kafka-topics.sh --bootstrap-server kafka-1:9092 --describe --topic order-events
```

어느 broker가 각 partition leader인지 먼저 확인한 뒤 그 broker를 중단해야 leader election을 관찰할 수 있다. follower를 멈췄는데 leader가 안 바뀌는 것은 정상이다.

## 실패 실험

- leader가 아닌 broker를 중단한 경우와 leader를 중단한 경우를 비교한다.
- 두 broker를 중단해 ISR가 줄어드는 상태를 기록한다. step-07의 `min.insync.replicas` 검증 전에는 성공/실패만으로 내구성을 판단하지 않는다.

## 다음 단계로 넘기는 상태

복제 상태를 볼 수 있게 됐다. step-07에서 `acks=all`과 `min.insync.replicas`로 ISR 감소 시 producer가 어떤 결과를 내야 하는지 설정한다.
