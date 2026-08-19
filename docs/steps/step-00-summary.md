---
title: Step 00 완료 정리 - 단일 Kafka 주문 이벤트 첫 연결
status: completed
step: 00
completed_at: 2026-08-19
---

# Step 00 완료 정리 - 단일 Kafka 주문 이벤트 첫 연결

> 이 문서는 해당 step의 코드 구현과 실험이 끝난 뒤 작성한다. 예상 결과가 아닌 실제 테스트 결과와 실행 환경 상태를 기록한다.

## 완료 상태

| 항목 | 내용 |
|---|---|
| 상태 | `completed` |
| 구현 일자 | 2026-08-19 |
| 실행 환경 | Java 21, Spring Boot 4.1.0, Gradle 9.5.1, Apache Kafka 4.1.2 Compose 구성 |
| 검증 명령 | `./gradlew.bat test`, `docker compose up -d`, Kafka CLI consumer |
| 검증 결과 | API 발행 단위 테스트 통과. 실제 broker에서 동일 key 3건이 partition 0의 offset 0, 1, 2에 기록됨. |

## 이번에 구현한 것

- 추가하거나 변경한 애플리케이션 코드: `POST /orders`가 `orderId`, 양수 `amount`를 받아 `OrderCreated` JSON을 `order-events`에 key=`orderId`로 발행한다. producer callback은 key, partition, offset 또는 실패를 로그로 남긴다.
- 변경한 설정과 인프라 구성: `docker-compose.yml`에 단일 Apache Kafka KRaft broker와 1-partition `order-events` 생성 컨테이너를 추가했다. host 앱은 `localhost:29092`, Compose 내부 초기화 컨테이너는 `kafka:9092` listener를 사용한다.
- 생성하거나 변경한 topic / consumer group / DB 테이블: topic=`order-events`, partition=1, consumer group=`order-observer`, DB 테이블 없음.
- 이전 step에서 받아온 동작: 시작 step이므로 없음.

## 사용자가 체감하는 변화

사용자는 `POST /orders` 호출로 주문 생성 요청과 함께 Kafka `OrderCreated` 발행을 요청할 수 있다. 정상 broker 환경에서는 producer와 observer 로그에서 같은 key의 partition·offset을 확인한다.

## 배운 핵심 개념

1. Kafka record는 topic, partition, offset, key, value로 구성되며 observer가 이 값을 로그로 남긴다.
2. `orderId`를 key로 사용하면 동일 주문 이벤트는 동일 partition으로 라우팅되어 partition 내부 순서를 관찰할 수 있다.
3. HTTP 202는 발행 요청 수락일 뿐이며, 실제 broker 기록 여부는 producer callback과 Kafka CLI로 확인해야 한다.

## 직접 확인한 사실

| 확인 항목 | 확인 방법 | 실제 결과 | 근거 |
|---|---|---|---|
| 주문 API의 Kafka 발행 요청 | `./gradlew.bat test --tests com.ssafy.kafkaorderlab.KafkaOrderLabApplicationTests` | 통과 | MockMvc 202 응답 및 `KafkaTemplate.send("order-events", "1001", JSON)` 호출 검증 |
| Compose 문법과 topic 생성 명령 | `docker compose config` | 통과 | `kafka-init`의 1 partition, replication factor 1 topic 생성 명령 확인 |
| broker와 topic 기동 | `docker compose up -d` 및 `kafka-topics.sh --describe` | 성공 | partition 1, leader 1, ISR 1 |
| 동일 key의 순서와 offset | `kafka-console-consumer.sh --partition 0 --offset 0 --max-messages 3` | 성공 | `1001` key가 P0 offset 0, 1, 2에 순서대로 기록 |

## 실패 실험과 결론

### 실험 1 - broker 중지 상태의 발행

- 설정: 단일 KRaft broker를 기동한다.
- 행동: broker를 중지하고 주문 API를 호출한다.
- 예상: producer callback 실패 로그가 남는다.
- 실제: 요청은 10초 내 응답하지 않았고 producer가 broker 재접속을 반복하며 연결 실패 경고를 남겼다.
- 결론: HTTP 성공 응답만으로 Kafka 기록 성공을 판단할 수 없다. 실제 결과는 `docs/experiments/step-00-broker-down.md`에 기록했다.

## 보장 범위와 한계

### 이번 step에서 확인한 보장

- API가 `orderId` key와 최소 `OrderCreated` JSON을 Kafka producer에 전달한다.
- Compose 구성은 단일 partition `order-events` 생성 명령을 포함한다.

### 아직 보장하지 못한 것

- producer idempotence, Kafka EOS, Outbox, Inbox에 의한 전달·업무 처리 보장

## 다음 step으로 갈 준비

- 이어받을 코드와 설정 상태: `order-events` 문자열 JSON 발행과 `order-observer` 로그 관찰자
- 다음 step에서 해결할 핵심 문제: JSON 문자열을 version이 있는 공통 event envelope 및 serializer/deserializer로 전환
- 다음 실습 전에 확인할 전제 조건: `docker compose up -d`로 broker와 topic을 기동

## 참고

- 예상과 달랐던 점: broker 중지 시 KafkaTemplate의 metadata 조회가 기본값으로 동기 대기해 HTTP 응답도 지연됐다.
- 다시 구현한다면 바꿀 점: 다음 단계의 retry/timeout 실습에서 producer timeout을 명시적으로 다룬다.
- 추가로 조사할 항목: step-01의 event envelope와 JSON serializer/deserializer 경계.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
