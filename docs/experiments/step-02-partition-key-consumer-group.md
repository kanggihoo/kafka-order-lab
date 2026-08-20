# Step 02 — Partition, key, consumer group 실험

## 목적

`order-events`와 `payment-events`를 3 partition으로 운영하면서 `orderId` key의 partition 고정, consumer 수에 따른 할당 변화, key 부재와 과도한 concurrency의 한계를 확인한다.

## 실행 상태

| 항목 | 내용 |
|---|---|
| 실행 일시 | 2026-08-20 (초회), 2026-08-20 재검증 |
| 실행 환경 | Gradle toolchain Java 21, 실행 JDK OpenJDK 25.0.1, Spring Boot 4.1.0, Apache Kafka 4.1.2, Docker Compose v5.1.2 |
| broker 상태 | `kafka-order-lab-kafka-1` healthy (단일 KRaft 노드) |
| topic 상태 | `order-events` PartitionCount 3, `payment-events` PartitionCount 3, ReplicationFactor 1 |
| consumer group | `order-observer`, `payment-service`, `payment-observer` |
| 실행 방식 | `java -jar build/libs/kafka-order-lab-0.0.1-SNAPSHOT.jar`, `SERVER_PORT`로 인스턴스 분리 |
| 최종 상태 | key 라우팅, 1~4 인스턴스 할당, key=null, concurrency 4 실험 모두 완료 |

실행 절차는 [step-02-runbook](../steps/step-02-runbook.md)과 같다.

## 실행 중 발견하고 해결한 환경 문제

`docker compose up -d`가 broker 기동 단계에서 실패했다.

```text
ERROR Encountered fatal fault: caught exception (org.apache.kafka.server.fault.ProcessTerminatingFaultHandler)
java.net.UnknownHostException: 2d4df13d3b64: 2d4df13d3b64: Try again
	at org.apache.kafka.metadata.ListenerInfo.withWildcardHostnamesResolved(ListenerInfo.java:237)
	at kafka.server.ControllerServer.startup(ControllerServer.scala:177)
```

`KAFKA_LISTENERS`가 와일드카드 host이므로 controller 기동 시 `InetAddress.getLocalHost()`로 자기 host 이름을 해석하는데, 컨테이너 ID가 해석되지 않아 프로세스가 종료됐다. compose에 `hostname: kafka`를 추가해 해결했다. 재시도만으로는 복구되지 않았고, hostname 고정 후 첫 시도에서 healthy가 됐다.

## 1. 같은 key의 partition 고정

행동: `1001`에 `OrderCreated` 3건, `1002`~`1010`에 각 1건을 `POST /orders`로 발행했다(모두 `202 Accepted`).

- 예상: 같은 `orderId`는 같은 partition으로, 다른 `orderId`는 여러 partition으로 분산된다.
- 실제:

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

- 결론: `orderId`를 key로 쓰면 같은 주문은 항상 같은 partition으로 간다. partition 수가 같은 두 topic에서는 같은 key가 같은 partition 번호를 받으므로, `order-events`와 `payment-events`의 partition 번호가 12건 모두 일치했다. 다만 key 분포는 균등하지 않았다. 12건이 P0 6건, P1 4건, P2 2건으로 나뉘어 hot partition이 쉽게 생길 수 있음을 보였다.

## 2. 인스턴스 1~4개의 partition 할당

행동: 같은 jar를 `SERVER_PORT` 8080 → 8081 → 8082 → 8083 순서로 추가 실행하고, 매번 `payment-service` group의 할당을 확인했다.

- 예상: partition 3개이므로 consumer 3개까지는 1:1로 나뉘고, 4번째는 할당을 받지 못한다.
- 실제:

| 인스턴스 수 | 할당 결과 |
|---:|---|
| 1 | 한 consumer가 `[order-events-0, order-events-1, order-events-2]` 전부 담당 |
| 2 | `[order-events-0, order-events-1]` / `[order-events-2]` |
| 3 | P0 / P1 / P2 각 1개 |
| 4 | 3개 consumer가 1개씩, 1개 consumer는 `#PARTITIONS 0` |

4개 인스턴스에서의 `--describe --members` 출력이다.

```text
GROUP           CONSUMER-ID                                                     CLIENT-ID                  #PARTITIONS
payment-service consumer-payment-service-1-a0ac3265-...                         consumer-payment-service-1 0
payment-service consumer-payment-service-1-04415dc9-...                         consumer-payment-service-1 1
payment-service consumer-payment-service-1-53bcc9b2-...                         consumer-payment-service-1 1
payment-service consumer-payment-service-1-9e2e299f-...                         consumer-payment-service-1 1
```

인스턴스가 추가될 때마다 기존 인스턴스에서 revoke → assign 로그가 남았다.

```text
Revoked partitions: group=payment-service, partitions=[order-events-0, order-events-1, order-events-2]
Assigned partitions: group=payment-service, partitions=[order-events-2]
```

- 결론: 병렬성 상한은 consumer 수가 아니라 partition 수다. consumer를 partition 수보다 늘리면 idle consumer만 늘고 처리량은 그대로다. 인스턴스 추가는 무료가 아니라 rebalance(revoke + 재할당)를 유발한다.

## 3. 같은 key 이벤트의 단일 consumer 순서 처리

행동: 인스턴스 4개가 떠 있는 상태에서 `1001`에 `OrderCreated` 3건을 추가 발행했다.

- 예상: 3건 모두 P0로 가고, P0을 소유한 인스턴스 하나가 offset 순서대로 처리한다.
- 실제: `order-events-0` offset 6, 7, 8에 기록되고 8083 인스턴스 한 곳에서만 offset 순서대로 소비됐다. 다른 3개 인스턴스에는 해당 로그가 없다.

```text
order created consumed by payment-service: key=1001, topic=order-events, partition=0, offset=6
order created consumed by payment-service: key=1001, topic=order-events, partition=0, offset=7
order created consumed by payment-service: key=1001, topic=order-events, partition=0, offset=8
```

- 결론: consumer group에서 partition 하나는 consumer 하나에만 할당되므로, 같은 key의 이벤트는 인스턴스를 늘려도 동시에 처리되지 않고 순서가 유지된다.

## 실패 실험 1 — key를 null로 발행

설정: 인스턴스 4개 실행, `payment-service`가 P0/P1/P2를 각각 다른 인스턴스에 할당한 상태.

행동: 애플리케이션 producer는 항상 `orderId`를 key로 쓰므로, `kafka-console-producer.sh`로 key 없이 같은 주문(`payload.orderId=1001`) 이벤트 9건을 발행했다. 6건은 한 번의 producer 실행으로, 3건은 producer를 3번 따로 실행해 보냈다.

- 예상: key가 없으면 partition이 고정되지 않아 같은 주문의 이벤트가 여러 partition으로 흩어지고, 서로 다른 consumer가 동시에 처리해 주문 단위 순서가 깨진다.
- 실제: 한 번에 보낸 6건은 sticky partitioner 때문에 모두 `order-events-2`로 갔고, producer를 따로 실행한 3건 중 1건도 P2, 2건은 `order-events-1`로 갔다. 결과적으로 같은 `1001` 주문의 이벤트가 P2 7건과 P1 2건으로 쪼개져 8080 인스턴스와 8082 인스턴스에서 **동시에** 처리됐다.

```text
# 8080 인스턴스 (P2 소유)
order created consumed by payment-service: key=null, topic=order-events, partition=2, offset=2..8
# 8082 인스턴스 (P1 소유)
order created consumed by payment-service: key=null, topic=order-events, partition=1, offset=4
order created consumed by payment-service: key=null, topic=order-events, partition=1, offset=5
```

후속 `PaymentRequested`는 payload의 `orderId`로 key를 복원했기 때문에 모두 `payment-events-0`에 들어갔지만, 두 인스턴스가 각자 발행했기 때문에 P0 안의 offset 순서가 원본 발생 순서와 일치하지 않았다. 8080이 offset 9~14와 17을, 8082가 offset 15~16을 기록해 서로 끼어들었다.

> 재검증 시 partition 번호는 producer 실행 시점의 sticky batch 경계에 따라 달라진다. 핵심은 "같은 주문이 둘 이상의 partition으로 쪼개져 여러 consumer가 동시에 처리한다"는 사실이고, 이는 두 번의 실행에서 모두 재현됐다.

- 결론: 순서 보장은 key가 있어야 성립한다. key가 없으면 partitioner가 batch 단위로 partition을 고르기 때문에 "우연히 같은 partition"이 되기도 하고, producer 실행이 바뀌면 흩어진다. 상류에서 한 번 순서가 깨지면 하류에서 key를 복원해도 순서는 복원되지 않는다.

## 실패 실험 2 — concurrency를 partition 수보다 크게 설정

설정: 인스턴스 1개, `PAYMENT_SERVICE_CONCURRENCY=4`.

행동: `payment-service` 리스너 container thread를 4개로 띄우고 `2001`~`2009` 주문 9건을 발행했다.

- 예상: thread는 4개지만 partition이 3개이므로 한 thread는 할당을 받지 못하고 추가 병렬성이 생기지 않는다.
- 실제: 애플리케이션 하나가 group member 4개(`consumer-payment-service-1`~`-4`)로 참여했고, 한 thread의 할당 목록이 비어 있었다.

```text
Assigned partitions: group=payment-service, partitions=[]
Assigned partitions: group=payment-service, partitions=[order-events-0]
Assigned partitions: group=payment-service, partitions=[order-events-2]
Assigned partitions: group=payment-service, partitions=[order-events-1]
```

`--describe --members`에서도 4 member 중 하나가 `#PARTITIONS 0`이었다. 주문 9건은 key 해시 결과 P0 6건과 P2 3건에만 들어가 container thread 2개(`ntainer#2-0-C-1` 6건, `ntainer#2-2-C-1` 3건)가 처리했고, 나머지 2개 thread는 아무 record도 처리하지 않았다.

- 결론: concurrency는 인스턴스 안의 consumer 수일 뿐이므로 partition 수를 넘으면 idle consumer가 된다. 게다가 partition이 남아 있어도 key 분포가 치우치면 실제 처리 thread 수는 partition 수보다 더 적어진다. 처리량 목표는 partition 수, key 분포, record당 처리 시간을 함께 봐야 한다.

## 최종 관찰

| 확인 항목 | 확인 방법 | 실제 결과 |
|---|---|---|
| topic partition 수 | `kafka-topics.sh --describe` | 두 topic 모두 3 |
| 같은 key의 partition 고정 | 애플리케이션 로그 | `1001` 6건(초기 3건 + 추가 3건) 전부 P0 |
| topic 간 partition 일치 | 애플리케이션 로그 | 12건 모두 order/payment partition 번호 동일 |
| 1·2·3·4 인스턴스 할당 | `kafka-consumer-groups.sh --describe`, `--members` | 3:0:0 → 2:1 → 1:1:1 → 1:1:1:0 |
| key=null 분산 | console-producer + 로그 | 같은 주문이 P2 7건 / P1 2건으로 분리, 두 인스턴스가 동시 처리 |
| concurrency 4 | `Assigned partitions` 로그 | 한 thread의 할당이 빈 목록 |

자동화 검증은 `PartitionRoutingIntegrationTest`(embedded Kafka, 3 partition)로 같은 key의 partition 고정과 topic 간 partition 일치를 확인한다.

## 보장 범위와 한계

확인한 보장

- 같은 key는 같은 partition으로 가고, partition 하나는 group 안에서 consumer 하나만 읽는다. 그래서 주문 단위 순서가 유지된다.
- partition 수가 group의 병렬성 상한이다.

아직 보장하지 못하는 것

- offset commit 시점을 통제하지 않으므로, rebalance나 강제 종료 시 중복 처리와 유실 가능성이 남아 있다. 이번 실험에서도 인스턴스를 추가할 때마다 revoke/assign이 발생했지만 commit 시점을 관찰하지 않았다.
- `payment-service`가 발행에 실패하면 재시도·DLT 없이 로그만 남는다.
- key가 없는 외부 record를 거부하는 정책이 없다.

## 다음 단계

step-03에서 수동 commit과 강제 종료로 중복·유실을 관찰한다.
