---
title: Step 02 완료 정리 — Partition, key, consumer group
status: completed
step: 02
completed_at: 2026-08-20
---

# Step 02 완료 정리 — Partition, key, consumer group

`order-events`와 `payment-events`를 3 partition으로 운영하면서 `orderId` key의 partition 고정, 인스턴스 1~4개의 할당 변화, key 부재와 과도한 concurrency의 한계를 실행 중인 broker에서 재검증했다.

## 완료 상태

| 항목 | 내용 |
|---|---|
| 상태 | `completed` |
| 구현 날짜 | 2026-08-20 |
| 실행 환경 | Gradle toolchain Java 21, 실행 JDK OpenJDK 25.0.1, Spring Boot 4.1.0, Gradle 9.5.1, Apache Kafka 4.1.2 (단일 KRaft 노드), Docker Compose v5.1.2 |
| 검증 명령 | `docker compose down -v && docker compose up -d`, `kafka-topics.sh --describe`, `./gradlew bootJar test`, `SERVER_PORT=808x java -jar ...`, `kafka-consumer-groups.sh --describe [--members]`, `kafka-console-producer.sh` |
| 검증 결과 | 자동 테스트 23개 성공, 실패 0개, 건너뜀 0개. 실제 broker에서 key 라우팅 12건, 인스턴스 1~4개 할당, key=null 분산, concurrency 4 idle consumer를 모두 재현함. |

> 이번 재검증은 `docker compose down -v`로 topic과 offset을 비운 새 환경에서 처음부터 다시 실행했다. 아래 모든 offset은 이 새 실행 기준이다.

## 이번에 구현한 것

- 추가하거나 변경한 애플리케이션 코드:
  - `PaymentRequestListener`가 `order-events`의 `OrderCreated`를 `payment-service` group으로 받아 `PaymentRequested`를 발행한다.
  - `PaymentEventService`가 payload의 `orderId`를 그대로 record key로 써서 `payment-events`에 발행한다.
  - `PaymentRequestedPayload`, `PaymentRequestedEventContract`, `PaymentRequestedEventValidator`로 결제 요청 이벤트 계약을 step-01과 같은 형식으로 정의했다.
  - `PaymentEventObserver`가 `payment-events`를 `payment-observer` group으로 소비해 key/partition/offset을 기록한다.
  - `AssignedPartitionsLogger`가 `ConsumerRebalanceListener` 이벤트를 받아 group, memberId, 할당·회수된 partition 목록을 로그로 남긴다.
- 변경한 설정과 인프라 구성:
  - `docker-compose.yml`에 `kafka-init` 서비스를 두어 `order-events`, `payment-events`를 3 partition으로 생성한다.
  - broker의 `hostname: kafka`를 고정해 controller 기동 시 host 이름 해석 실패를 막았다.
  - `server.port`를 `SERVER_PORT`로, 세 consumer group의 concurrency를 각각 환경변수로 덮어쓸 수 있게 했다.
- 생성·변경한 topic / consumer group / DB 테이블:
  - `order-events`를 1 partition에서 3 partition으로 재생성했다.
  - `payment-events`를 3 partition으로 새로 만들었다.
  - consumer group `payment-service`, `payment-observer`를 추가했다(기존 `order-observer` 유지).
  - DB는 아직 도입하지 않는다.
- 이전 step에서 바뀐 동작:
  - step-01은 발행과 관찰만 했지만, 이제 consumer가 이벤트를 받아 다음 이벤트를 발행하는 연쇄가 생겼다.
  - partition이 3개가 되어 서로 다른 주문이 병렬로 처리되고, 인스턴스를 늘리면 rebalance가 발생한다.

## 사용자가 체감하는 변화

서로 다른 주문은 여러 partition에 나뉘어 병렬로 결제 요청까지 진행되지만, 같은 `orderId`의 이벤트는 인스턴스를 4개까지 늘려도 항상 한 인스턴스에서 발행 순서대로 처리된다. 처리량을 늘리려고 인스턴스를 partition 수보다 많이 띄우면 늘어난 인스턴스는 아무 일도 하지 않는다.

## 배운 핵심 개념

1. **key가 partition을 결정하고, partition이 순서 단위다.** `orderId`를 key로 준 12건에서 `1001` 3건이 모두 P0으로 갔고, 같은 key는 재실행 뒤에도 같은 partition을 받았다. 순서 보장의 범위는 topic 전체가 아니라 partition 하나다.
2. **partition 수가 consumer group의 병렬성 상한이다.** 인스턴스를 4개로 늘렸을 때 `--members` 출력의 마지막 consumer가 `#PARTITIONS 0`이었다. consumer를 늘려도 partition보다 많아지면 idle consumer만 생긴다.
3. **partition 하나는 group 안에서 consumer 하나만 읽는다.** 인스턴스 4개 상태에서 `1001` 3건이 P0 offset 6,7,8에 들어갔고 P0을 소유한 8083 인스턴스 한 곳에서만 순서대로 소비됐다. 나머지 3개 인스턴스 로그에는 해당 record가 없다.
4. **concurrency는 인스턴스 안의 consumer 수일 뿐이다.** `PAYMENT_SERVICE_CONCURRENCY=4`인 인스턴스 하나가 group member 4개로 참여했고, 인스턴스를 4개 띄운 것과 같은 결과(1개 idle)가 나왔다.
5. **partition 수가 같은 두 topic에서는 같은 key가 같은 partition 번호를 받는다.** 12건 모두 `order-events`와 `payment-events`의 partition 번호가 일치했다. 같은 murmur2 해시와 같은 partition 수를 쓰기 때문이다.

## 직접 확인한 사실

| 확인 항목 | 확인 방법 | 실제 결과 | 근거 |
|---|---|---|---|
| topic partition 수 | `kafka-topics.sh --describe` | 두 topic 모두 `PartitionCount: 3`, `ReplicationFactor: 1` | `docker compose exec -T kafka ... --describe --topic order-events` |
| 자동 테스트 | `./gradlew bootJar test` | 23개 성공, 실패 0, 건너뜀 0 | `build/test-results/test/*.xml` |
| 같은 key의 partition 고정 | 애플리케이션 로그 | `1001` 3건 전부 `order-events` P0, `payment-events` P0 | `order/payment event published: key=1001, ... partition=0` |
| topic 간 partition 일치 | 애플리케이션 로그 | 12건 모두 두 topic의 partition 번호 동일 | 아래 [실험 결과 표](#1-같은-key의-partition-고정) |
| key 분포 불균등 | 애플리케이션 로그 | 12건이 P0 6 / P1 4 / P2 2 | 같은 표 |
| 인스턴스 1~4개 할당 | `kafka-consumer-groups.sh --describe`, `--members` | 3:0:0 → 2:1 → 1:1:1 → 1:1:1:0 | 아래 [할당 표](#2-인스턴스-14개의-partition-할당) |
| rebalance 발생 | 애플리케이션 로그 | 인스턴스 추가마다 revoke → assign 기록 | `Revoked/Assigned partitions: group=payment-service` |
| 같은 key의 단일 consumer 처리 | 애플리케이션 로그 | P0 offset 6,7,8을 8083 인스턴스만 순서대로 소비 | 인스턴스별 `key=1001` 로그 개수 비교 |
| key=null 분산 | `kafka-console-producer.sh` + 로그 | 같은 주문 9건이 P2 7건 / P1 2건으로 분리, 두 인스턴스가 동시 처리 | 아래 [실험 1](#실험-1--key를-null로-발행) |
| concurrency 4의 idle consumer | `Assigned partitions` 로그, `--members` | member 4개 중 1개가 빈 할당 / `#PARTITIONS 0` | 아래 [실험 2](#실험-2--concurrency를-partition-수보다-크게-설정) |

전체 실험 기록과 로그 인용은 `docs/experiments/step-02-partition-key-consumer-group.md`에 있다.

### 1. 같은 key의 partition 고정

`1001`에 3건, `1002`~`1010`에 각 1건을 `POST /orders`로 발행했다(모두 `202 Accepted`).

| key | order-events partition | payment-events partition |
|---|---:|---:|
| 1001 (3건) | 0, 0, 0 | 0, 0, 0 |
| 1002 | 0 | 0 |
| 1003 | 1 | 1 |
| 1004 | 1 | 1 |
| 1005 | 2 | 2 |
| 1006 | 2 | 2 |
| 1007 | 0 | 0 |
| 1008 | 0 | 0 |
| 1009 | 1 | 1 |
| 1010 | 1 | 1 |

### 2. 인스턴스 1~4개의 partition 할당

같은 jar를 `SERVER_PORT` 8080 → 8081 → 8082 → 8083 순서로 추가 실행했다.

| 인스턴스 수 | `payment-service` 할당 |
|---:|---|
| 1 | 한 consumer가 `[order-events-0, order-events-1, order-events-2]` 전부 |
| 2 | `[P0, P1]` / `[P2]` |
| 3 | P0 / P1 / P2 각 1개 |
| 4 | 3개 consumer가 1개씩, 1개는 `#PARTITIONS 0` |

인스턴스 추가마다 남은 revoke/assign 기록이다.

```text
# 8080 인스턴스
Assigned [order-events-0, order-events-1, order-events-2]
Revoked  [order-events-0, order-events-1, order-events-2]
Assigned [order-events-0, order-events-1]
Revoked  [order-events-0, order-events-1]
Assigned [order-events-1]
Revoked  [order-events-1]
Assigned [order-events-2]
```

## 실패 실험과 결론

### 실험 1 — key를 null로 발행

- 설정: 인스턴스 4개 실행, `payment-service`가 P0/P1/P2를 서로 다른 인스턴스에 할당한 상태.
- 행동: 애플리케이션 producer는 항상 `orderId`를 key로 쓰므로 `kafka-console-producer.sh`로 key 없이 같은 주문(`payload.orderId=1001`) 이벤트 9건을 발행했다. 6건은 한 번의 producer 실행으로, 3건은 producer를 3번 따로 실행해 보냈다.
- 예상: key가 없으면 partition이 고정되지 않아 같은 주문의 이벤트가 여러 partition으로 흩어지고, 서로 다른 consumer가 동시에 처리해 주문 단위 순서가 깨진다.
- 실제: 한 번에 보낸 6건은 sticky partitioner 때문에 모두 `order-events-2`로 갔고, 따로 실행한 3건 중 1건도 P2, 2건은 P1로 갔다. 결과적으로 같은 `1001` 주문이 P2 7건과 P1 2건으로 쪼개져 8080 인스턴스와 8082 인스턴스에서 **동시에** 처리됐다.

```text
# 8080 인스턴스 (P2 소유)
order created consumed by payment-service: key=null, topic=order-events, partition=2, offset=2..8
# 8082 인스턴스 (P1 소유)
order created consumed by payment-service: key=null, topic=order-events, partition=1, offset=4
order created consumed by payment-service: key=null, topic=order-events, partition=1, offset=5
```

후속 `PaymentRequested`는 payload의 `orderId`로 key를 복원했기 때문에 모두 `payment-events-0`에 들어갔지만, 두 인스턴스가 각자 발행했기 때문에 P0 안의 offset 순서가 원본 발생 순서와 일치하지 않았다. 8080이 offset 9~14와 17을, 8082가 offset 15~16을 기록해 서로 끼어들었다.

- 결론: 순서 보장은 key가 있어야 성립한다. key가 없으면 partitioner가 batch 단위로 partition을 고르기 때문에 "우연히 같은 partition"이 되기도 하고 producer 실행이 바뀌면 흩어진다. 상류에서 한 번 순서가 깨지면 하류에서 key를 복원해도 순서는 복원되지 않는다.

### 실험 2 — concurrency를 partition 수보다 크게 설정

- 설정: 인스턴스 1개, `PAYMENT_SERVICE_CONCURRENCY=4`.
- 행동: `payment-service` 리스너 container thread를 4개로 띄우고 `2001`~`2009` 주문 9건을 발행했다.
- 예상: thread는 4개지만 partition이 3개이므로 한 thread는 할당을 받지 못하고 추가 병렬성이 생기지 않는다.
- 실제: 애플리케이션 하나가 group member 4개(`consumer-payment-service-1`~`-4`)로 참여했고, 한 thread의 할당 목록이 비어 있었다.

```text
Assigned partitions: group=payment-service, partitions=[]
Assigned partitions: group=payment-service, partitions=[order-events-2]
Assigned partitions: group=payment-service, partitions=[order-events-0]
Assigned partitions: group=payment-service, partitions=[order-events-1]
```

`--describe --members`에서도 4 member 중 하나가 `#PARTITIONS 0`이었다. 게다가 `2001`~`2009` 9건은 key 해시 결과 P0 6건과 P2 3건에만 들어가 container thread 2개(`ntainer#2-0-C-1` 6건, `ntainer#2-2-C-1` 3건)만 일했고 나머지 2개 thread는 아무 record도 처리하지 않았다.

- 결론: concurrency는 인스턴스 안의 consumer 수일 뿐이므로 partition 수를 넘으면 idle consumer가 된다. 게다가 partition이 남아 있어도 key 분포가 치우치면 실제 처리 thread 수는 partition 수보다 더 적어진다. 처리량 목표는 partition 수, key 분포, record당 처리 시간을 함께 봐야 한다.

## 보장 범위와 한계

### 이번 step에서 확인한 보장

- 같은 key(=`orderId`)를 준 record는 항상 같은 partition으로 간다.
- partition 하나는 consumer group 안에서 consumer 하나만 읽으므로, 같은 주문의 이벤트는 인스턴스를 늘려도 동시에 처리되지 않고 partition 내 offset 순서가 유지된다.
- partition 수가 consumer group의 병렬성 상한이다. 인스턴스든 concurrency든 그 위로는 idle consumer만 늘어난다.
- partition 수가 같은 두 topic에서는 같은 key가 같은 partition 번호를 받는다.

### 아직 보장하지 못하는 것

- **offset commit 시점을 통제하지 않는다.** 기본 auto commit이므로 rebalance나 강제 종료 시 중복 처리와 유실 가능성이 남아 있다. 이번 실험에서 인스턴스를 추가할 때마다 revoke/assign이 발생했지만 commit 시점은 관찰하지 않았다.
- **재시도와 DLT가 없다.** `payment-service`가 `payment-events` 발행에 실패하면 로그만 남고 이벤트가 사라진다.
- **key 없는 외부 record를 거부하는 정책이 없다.** 실험 1처럼 CLI로 들어온 `key=null` record도 그대로 소비해 순서가 깨진 채 하류로 흘렀다.
- **중복 제거가 없다.** `eventId`가 있지만 이를 이용한 idempotent 처리는 하지 않으므로 같은 이벤트가 두 번 오면 두 번 처리된다.
- 이번 step의 순서 보장은 **partition 라우팅에서 오는 것**이고, producer idempotence나 Kafka EOS, Outbox/Inbox와는 무관하다. 이들은 각각 step-07, step-08, step-09, step-10의 범위다.

## 다음 step으로 갈 준비

- 이어받을 코드·설정 상태: 3 partition의 `order-events`/`payment-events`, `order-observer`·`payment-service`·`payment-observer` 세 group, `AssignedPartitionsLogger`로 rebalance를 관찰할 수 있는 상태. `SERVER_PORT`와 concurrency 환경변수로 인스턴스·thread 수를 조절할 수 있다.
- 다음 step에서 해결할 남은 문제: auto commit이라 중복·유실 가능성이 통제되지 않는다. 수동 commit으로 바꾸고, 처리 후 commit 전에 강제 종료해 중복을 재현해야 한다.
- 다음 실습 전에 확인할 전제 조건: `docker compose down -v`로 offset을 비운 뒤 시작할지, 기존 offset을 유지할지 정해야 한다. lag 관찰을 위해 `kafka-consumer-groups.sh --describe`의 `CURRENT-OFFSET`/`LOG-END-OFFSET`/`LAG` 열을 읽는 데 익숙해져야 한다.

## 회고

- 예상과 달랐던 점:
  - key=null이 곧바로 균등 분산을 뜻하지 않았다. sticky partitioner 때문에 한 번에 보낸 6건이 모두 같은 partition으로 갔고, producer 실행을 나눴을 때야 흩어졌다. "key가 없으면 순서가 깨진다"는 것은 확률적으로 그렇다는 뜻이지 매번 깨진다는 뜻이 아니다.
  - partition 3개에 12건을 보냈는데 6:4:2로 치우쳤다. partition 수를 늘려도 key 분포가 나쁘면 병렬성이 그대로일 수 있다는 점이 실험 2에서 더 분명해졌다(9건이 2개 partition에만 들어가 thread 2개만 일했다).
  - 인스턴스를 4개까지 늘리는 과정에서 8080 인스턴스가 P0,P1,P2 → P0,P1 → P1 → P2로 네 번 재할당됐다. rebalance가 단순히 "새 인스턴스에 하나 떼어주기"가 아니라 기존 소유권까지 뒤섞는다.
- 다시 구현한다면 바꿀 점:
  - `hostname: kafka`를 처음부터 넣어야 했다. compose 기동 실패의 원인이 재시도로 해결되지 않는 종류라는 걸 늦게 알았다.
  - 인스턴스별 로그를 파일로 분리해 두면 partition 소유자를 찾기 쉽다. 이번에도 포트별 로그 파일을 나눠 두어 `key=1001` 소비 인스턴스를 바로 특정할 수 있었다.
  - 실행 JDK(25)와 Gradle toolchain(21)이 달랐다. 실습 환경은 한 버전으로 맞추는 편이 결과 재현에 안전하다.
- 추가로 조사할 항목:
  - `partition.assignment.strategy`를 range에서 cooperative sticky로 바꾸면 revoke 범위가 줄어드는지.
  - sticky partitioner의 batch 전환 기준(`linger.ms`, batch size)이 key=null 분산에 어떻게 작용하는지.
  - partition 수를 나중에 늘리면 기존 key의 partition이 바뀌어 순서 보장이 깨지는 문제를 어떻게 다루는지.
