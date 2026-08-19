# Step 00 — 단일 Kafka와 주문 이벤트의 첫 연결

## 사용자가 체감하는 변화

사용자가 `POST /orders`를 호출하면 주문이 생성되고 `OrderCreated` 이벤트가 Kafka의 `order-events`에 남는다. 애플리케이션 로그와 CLI에서 같은 주문의 key, partition, offset을 확인한다.

## 목표와 완료 조건

- 단일 KRaft broker와 Spring Boot 4.1 애플리케이션을 Docker Compose로 실행한다.
- `orderId`를 record key로 사용해 `OrderCreated`를 발행하고 소비한다.
- API 호출 3회에 대해 topic/partition/offset을 기록한다.

## 시작 상태와 이번 변경

시작 상태는 빈 Spring Boot 프로젝트다. `order-events` 토픽(처음에는 1 partition)과 주문 생성 API, producer, 관찰 전용 consumer를 추가한다. 이 단계에서는 DB·결제·재시도·다중 broker를 도입하지 않는다.

## 핵심 이론

Kafka record는 topic, partition, offset, key, value를 가진다. offset은 partition 안에서만 증가하며, key가 같은 record는 같은 partition으로 라우팅된다. 따라서 전역 순서가 아니라 **한 partition 내부 순서**만 보장된다.

## 구현 순서

1. `docker-compose.yml`에 Kafka KRaft broker를 추가하고 `order-events` 토픽을 1 partition으로 생성한다.
2. Gradle에 `spring-kafka`, Spring Web, Actuator 의존성을 추가한다.
3. `POST /orders`가 `orderId`, `amount`를 받아 `OrderCreated`를 만든다. `orderId`는 UUID 또는 증가값 중 하나로 일관되게 정한다.
4. `KafkaTemplate<String, String>`으로 key=`orderId`, value=간단한 JSON을 발행한다.
5. `@KafkaListener(groupId = "order-observer")`에서 topic, partition, offset, key, value를 구조화 로그로 남긴다.
6. 발행 성공 콜백에도 partition/offset을 남긴다. producer callback과 consumer 로그를 혼동하지 않는다.

최소 value 예시:

```json
{"orderId":"1001","amount":15000,"type":"OrderCreated"}
```

## 실행·검증

```bash
docker compose up -d
./gradlew bootRun
curl -X POST http://localhost:8080/orders -H "Content-Type: application/json" -d '{"orderId":"1001","amount":15000}'
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic order-events --from-beginning --property print.key=true --property print.partition=true --property print.offset=true
```

동일한 `orderId=1001`을 세 번 보내면 현재는 모두 P0에 쌓인다. offset은 0, 1, 2처럼 증가한다. HTTP 성공만으로 Kafka 발행 성공을 단정하지 말고 callback 로그와 CLI를 함께 확인한다.

## 실패 실험

- broker를 중지한 뒤 주문을 생성해 producer 실패를 관찰한다.
- value를 JSON이 아닌 문자열로 보내고, 아직 계약 검증이 없음을 확인한다.

`docs/experiments/step-00-broker-down.md`에 설정, 행동, 예상, 실제, 결론을 기록한다.

## 다음 단계로 넘기는 상태

문자열 JSON을 임시로 사용했다. step-01에서 모든 이벤트를 공통 envelope와 명시적 JSON serializer/deserializer로 바꾼다.


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
