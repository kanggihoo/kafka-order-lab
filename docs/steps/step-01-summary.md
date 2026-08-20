---
title: Step 01 완료 정리 - 이벤트 계약과 JSON 직렬화
status: completed
step: 01
completed_at: 2026-08-20
---

# Step 01 완료 정리 - 이벤트 계약과 JSON 직렬화

`OrderCreated`를 타입 있는 JSON 이벤트 계약으로 전환하고 v1·v2 호환성, 역직렬화 실패, 의미 검증 실패와 실제 재시도 동작을 자동 테스트와 실행 중인 Kafka에서 확인했다.

## 완료 상태

| 항목 | 내용 |
|---|---|
| 상태 | `completed` |
| 구현 날짜 | 2026-08-20 |
| 실행 환경 | Java 21.0.11, Spring Boot 4.1.0, Gradle 9.5.1, Apache Kafka 4.1.2 Compose 구성, Docker Compose 5.2.0 |
| 검증 명령 | `./gradlew.bat test`, `docker compose config --quiet` |
| 검증 결과 | 전체 테스트 15개 성공, 실패 0개, 건너뜀 0개. 실제 broker에서 v1·v2 처리, 문자열 `amount`의 반복 역직렬화 실패, 누락 `orderId`의 10회 시도 후 offset 진행을 확인함. |

## 이번에 구현한 것

- 추가하거나 변경한 애플리케이션 코드:
  - `EventEnvelope<T>`에 `eventId`, `eventType`, `eventVersion`, `occurredAt`, `payload`를 정의했다.
  - `OrderCreatedPayload`를 record로 만들고 v2의 선택 필드인 `couponCode`가 `null`이면 JSON에서 제외했다.
  - `OrderCreatedEventContract`에 이벤트 종류, 지원 버전, Kafka header 이름을 모아 producer와 consumer가 같은 계약값을 사용하게 했다.
  - `OrderEventService`가 `couponCode` 유무에 따라 v1 또는 v2 envelope를 만들고 `eventType`, `eventVersion` header와 함께 발행하도록 변경했다.
  - `OrderCreatedEventValidator`가 envelope 필수값, 지원 이벤트 종류·버전, payload의 `orderId`와 양수 `amount`를 검사하도록 구현했다.
  - `OrderEventObserver`가 역직렬화된 이벤트를 검증한 뒤 event 정보와 Kafka topic, partition, offset을 로그로 남기도록 구현했다.
- 변경한 설정과 인프라 구성:
  - producer value serializer를 `JacksonJsonSerializer`로 설정했다.
  - consumer는 목표 타입을 `EventEnvelope<OrderCreatedPayload>`로 고정하고 trusted package를 `com.ssafy.kafkaorderlab.event`로 제한했다.
  - 숫자 필드에 문자열이 들어왔을 때 Jackson이 자동 변환하지 않고 실패하도록 coercion을 설정했다.
- 생성·변경한 topic / consumer group / DB 테이블:
  - 기존 `order-events` 1 partition과 `order-observer` consumer group을 유지했다.
  - 새 topic과 DB 테이블은 추가하지 않았다.
- 이전 step에서 바뀐 동작:
  - 임시 JSON 문자열 대신 Java record 기반 envelope를 JSON으로 직렬화하고, consumer가 같은 타입으로 역직렬화한다.

## 사용자가 체감하는 변화

`POST /orders`의 요청과 `202 Accepted` 응답 방식은 유지된다. Kafka에 기록되는 값은 이벤트 식별자, 종류, 버전, 발생 시각을 가진 계약형 JSON으로 바뀌며, `couponCode`를 보내면 v2 이벤트가 발행된다.

## 배운 핵심 개념

1. **Event envelope는 모든 이벤트의 공통 문맥을 제공한다.** `eventId`는 중복 전달을 식별하고 `eventVersion`은 스키마 호환성을 판단하므로 서로 대체할 수 없다. 이 구조는 `EventEnvelope<T>`와 producer 테스트로 확인했다.
2. **호환성은 변경 방향과 consumer의 해석 방식으로 결정된다.** v2에 선택 필드 `couponCode`를 추가하고 구 consumer가 알 수 없는 필드를 무시하게 했을 때 v1 payload로 계속 읽을 수 있음을 `KafkaJsonConfigurationTest`로 확인했다.
3. **JSON 파싱 성공과 이벤트 계약 충족은 다르다.** `amount`의 문자열 입력은 deserializer 경계에서 거부하고, 누락된 `orderId`나 지원하지 않는 버전은 역직렬화 후 validator에서 거부하도록 경계를 나눴다.
4. **직렬화 타입과 신뢰 범위를 명시해야 한다.** consumer가 임의 타입 header에 의존하지 않도록 목표 타입을 고정하고 trusted package를 제한했다.

## 직접 확인한 사실

| 확인 항목 | 확인 방법 | 실제 결과 | 근거 |
|---|---|---|---|
| 전체 빌드와 자동 테스트 | `./gradlew.bat test` | 테스트 15개 성공, 실패 0개, 건너뜀 0개 | `build/test-results/test/`의 6개 test suite |
| v1·v2 producer 계약 | `OrderEventServiceTest` 3개 실행 | v1/v2 선택, envelope 필수값, event header 검증 성공 | `src/test/java/com/ssafy/kafkaorderlab/service/OrderEventServiceTest.java` |
| JSON 직렬화와 호환성 | `KafkaJsonConfigurationTest` 4개 실행 | v2 역직렬화, v1의 null 필드 생략, 구 consumer의 v2 선택 필드 무시 성공 | `src/test/java/com/ssafy/kafkaorderlab/config/KafkaJsonConfigurationTest.java` |
| 실제 Kafka record의 v1·v2 역직렬화 | embedded Kafka 통합 테스트 실행 | 두 record를 소비해 v1의 `couponCode=null`, v2의 `couponCode=WELCOME` 확인 | `src/test/java/com/ssafy/kafkaorderlab/OrderEventJsonIntegrationTest.java` |
| 소비 계약 검증 | `OrderCreatedEventValidatorTest` 4개 실행 | v1·v2 허용, 누락 필드와 미지원 v3 거부 확인 | `src/test/java/com/ssafy/kafkaorderlab/consumer/OrderCreatedEventValidatorTest.java` |
| 실제 broker의 v1·v2 record | API 발행 후 producer·observer 로그 확인 | offset 0의 v1과 offset 1의 v2를 observer가 처리했고 계약 header도 확인 | `docs/experiments/step-01-event-contract-json-serialization.md` |
| 실제 broker의 계약 위반 | Kafka CLI raw producer와 consumer group 조회 | offset 2의 역직렬화 실패는 무제한 반복, offset 3의 validator 실패는 10회 시도 후 offset 4 커밋 | `docs/experiments/step-01-event-contract-json-serialization.md` |
| Compose 구성 | `docker compose config --quiet` | 단일 broker용 offsets topic replication factor 1 적용 후 consumer group 정상 동작 | `docker-compose.yml` |

## 실패 실험과 결론

### 실험 1 - amount 타입을 숫자에서 문자열로 변경

- 설정: v2 JSON의 `"amount":15000`을 `"amount":"15000"`으로 변경했다.
- 행동: `JacksonJsonDeserializer<EventEnvelope<OrderCreatedPayload>>`로 역직렬화하는 자동 테스트를 실행했다.
- 예상: 숫자 필드의 문자열 강제 변환을 거부한다.
- 실제: `KafkaJsonConfigurationTest.rejectsStringAmount`가 역직렬화 예외 발생을 확인하고 통과했다.
- 결론: JSON 문법이 유효해도 필드 타입 변경은 계약을 깨뜨리며, 현재 consumer는 이를 역직렬화 단계에서 차단한다.

### 실험 2 - orderId 누락

- 설정: `OrderCreatedPayload`의 `orderId`를 `null`로 구성했다.
- 행동: 역직렬화 이후의 계약 검증기를 직접 실행했다.
- 예상: `orderId is required` 계약 위반 예외가 발생한다.
- 실제: `OrderCreatedEventValidatorTest.rejectsMissingOrderId`가 `InvalidOrderCreatedEventException`과 `orderId` 메시지를 확인하고 통과했다.
- 결론: 필드 누락은 JSON 역직렬화만으로 완전히 차단되지 않으므로 consumer의 의미 검증이 필요하다.

### 실험 3 - 실제 broker에 잘못된 raw JSON 발행

- 설정: `docs/experiments/step-01-event-contract-json-serialization.md`에 문자열 `amount`와 누락된 `orderId` 발행 명령을 준비했다.
- 행동: broker와 애플리케이션을 실행하고 문자열 `amount` record를 offset 2에, `orderId` 누락 record를 offset 3에 발행했다.
- 예상: deserializer 또는 validator 실패 후 retry/DLT 미설정 상태의 재전달 가능성을 관찰한다.
- 실제: 문자열 `amount`는 listener 전에 역직렬화가 실패해 offset 2에서 무제한 반복됐다. `orderId` 누락은 listener validator에서 실패해 총 10회 시도 후 DLT 없이 건너뛰었고 offset 4가 커밋됐다.
- 결론: 역직렬화 예외와 listener 예외는 현재 error handler에서 다르게 처리된다. poison pill 격리와 실패 record 보관은 이후 retry/DLT 단계에서 구현해야 한다.

## 보장 범위와 한계

### 이번 step에서 확인한 보장

- producer가 `OrderCreated` v1·v2 envelope와 계약 header를 생성한다.
- consumer가 타입 있는 JSON을 `EventEnvelope<OrderCreatedPayload>`로 역직렬화한다.
- v1 consumer 모델은 v2에서 추가된 선택 `couponCode`를 무시하고 기존 필드를 읽을 수 있다.
- 문자열 `amount`, 누락된 필수 envelope 값과 `orderId`, 미지원 버전은 자동 테스트에서 거부된다.

### 아직 보장하지 못하는 것

- `ErrorHandlingDeserializer`와 DLT를 이용한 역직렬화 poison pill 격리
- retry 횟수·간격, DLT 이동 및 poison pill 격리
- producer idempotence, Kafka EOS, Outbox, Inbox가 제공하는 전달 또는 업무 처리 보장
- 여러 partition과 consumer group 사이의 병렬 처리 및 순서

## 다음 step으로 갈 준비

- 이어받을 코드·설정 상태: 계약형 `OrderCreated` v1·v2 producer, typed JSON consumer, 1-partition `order-events`, `order-observer` group
- 다음 step에서 해결할 남은 문제: `order-events`를 3 partition으로 확장하고 같은 `orderId` key의 순서, consumer group 분배, `PaymentRequested` 발행 흐름을 검증한다.
- 다음 실습 전에 확인할 전제 조건: Compose의 단일 broker offsets topic 설정을 유지하고 `order-events`를 3 partition으로 변경할 때 consumer group 재분배를 관찰한다.

## 회고

- 예상과 달랐던 점: Jackson은 설정에 따라 숫자 문자열을 숫자로 강제 변환할 수 있어, 타입 변경 실패를 확실히 재현하려면 coercion을 명시적으로 차단해야 했다.
- 다시 구현한다면 바꿀 점: 자동 테스트와 수동 CLI 실험을 같은 시점에 실행해 완료 문서에 실제 broker 로그까지 함께 남긴다.
- 추가로 조사할 항목: Spring Kafka의 deserialization failure 처리 경로와 retry/DLT가 poison pill record를 다루는 방식.

## 최종 요약

Step 01에서는 `OrderCreated`를 공통 envelope와 버전을 가진 JSON 계약으로 전환하고 v1·v2의 하위 호환성을 자동 테스트와 실제 broker record로 확인했다. 필드 타입 오류는 deserializer에서, 필수값과 지원 버전 오류는 validator에서 분리해 차단한다. 역직렬화 poison pill은 같은 offset에서 무제한 반복되고 listener 검증 실패는 10회 시도 후 DLT 없이 건너뛰는 현재 한계까지 확인했으므로 Step 01은 완료 상태다.
