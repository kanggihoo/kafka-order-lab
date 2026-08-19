# Step 00 Runbook — Kafka 주문 이벤트 검증

Step 00의 정상 발행, Consumer 관찰, topic 상태, Broker 장애 동작을 명령어로 재현하는 실행 절차다.

## 사전 조건

- Docker Desktop 또는 Docker daemon이 실행 중이어야 한다.
- 프로젝트 루트에서 명령을 실행한다.
- Spring Boot 앱은 Docker Compose와 별도로 실행한다.

## 1. Docker Compose 설정 확인

```bash
docker info
docker compose config --quiet
```

`docker info`가 Docker Server 정보를 출력하고, `docker compose config --quiet`가 아무 출력 없이 종료되면 설정 문법이 정상이다.

## 2. Kafka와 topic 기동

```bash
docker compose up -d
docker compose ps -a
```

정상 상태:

- `kafka`: `Up ... (healthy)`
- `kafka-init`: `Exited (0)`

`kafka-init`은 topic 생성 명령을 수행한 뒤 종료되는 일회성 컨테이너다. `Exited (0)`은 실패가 아니라 정상 완료를 의미한다.

## 3. topic 상태 확인

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic order-events
```

Step 00의 기대 상태:

```text
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
Leader: 1
Isr: 1
```

현재 topic의 partition별 next offset은 다음 명령으로 확인한다.

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:9092 \
  --topic order-events
```

## 4. Spring Boot 실행

별도 터미널에서 프로젝트 루트에서 실행한다.

```bash
./gradlew bootRun
```

애플리케이션이 실행되면 다음을 확인한다.

```bash
curl -sS http://localhost:8080/actuator/health
```

예상 결과:

```json
{"status":"UP"}
```

애플리케이션 로그에 다음과 같은 Consumer 구독 로그가 남는다.

```text
Subscribed to topic(s): order-events
groupId=order-observer
```

## 5. 정상적인 주문 이벤트 발행

```bash
curl -sS \
  -w '\nHTTP %{http_code} TIME %{time_total}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"step00-normal","amount":1000}' \
  http://localhost:8080/orders
```

예상 결과:

```text
HTTP 202
```

Spring Boot 로그에서 다음 두 결과를 확인한다.

```text
order event published: key=step00-normal, partition=0, offset=...
order event observed: key=step00-normal, partition=0, offset=..., value=...
```

- `order event published`: Producer가 Broker 저장 응답을 받은 결과
- `order event observed`: Consumer Listener가 record를 읽은 결과

## 6. Kafka CLI로 record 확인

특정 partition과 offset부터 확인하면 기존 record가 많은 경우에도 원하는 범위를 명확히 볼 수 있다.

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic order-events \
  --partition 0 \
  --offset 0 \
  --max-messages 3 \
  --timeout-ms 10000 \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

예상 출력 형식:

```text
Partition:0 Offset:0 step00-normal {"orderId":"step00-normal","amount":1000,"type":"OrderCreated"}
```

record 개수를 미리 알 수 없으면 `--from-beginning`을 사용할 수 있다. 종료 조건이 없으면 계속 실행될 수 있으므로 `--timeout-ms` 또는 `--max-messages`를 함께 지정한다.

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic order-events \
  --from-beginning \
  --timeout-ms 10000 \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

## 7. Broker 장애 실험

이 절차는 정상 실행에 필요한 단계가 아니라 장애 동작을 관찰하기 위한 실험이다.

### Broker 중지

```bash
docker compose stop kafka
```

### Broker 중지 상태에서 API 호출

```bash
curl --max-time 12 -sS \
  -w '\nHTTP %{http_code} TIME %{time_total}s\n' \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"step00-broker-down","amount":1000}' \
  http://localhost:8080/orders
```

예상 결과:

```text
curl: (28) Operation timed out ...
HTTP 000
```

동시에 Spring Boot 로그에서 다음과 같은 Producer와 Consumer 재접속 로그를 확인한다.

```text
Bootstrap broker localhost:29092 disconnected
Connection to node ... could not be established
Node may not be available
Rebootstrapping with [localhost/127.0.0.1:29092]
```

### Broker 복구

```bash
docker compose start kafka
docker compose ps
```

`kafka`가 `healthy`가 된 뒤 topic 상태를 다시 확인한다.

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic order-events
```

Broker 복구 후 다음을 확인한다.

- 중지 중 전송된 요청의 Producer callback이 나중에 실행될 수 있음
- client는 이미 timeout됐어도 서버 내부 요청은 계속 진행될 수 있음
- Broker 복구 후 새 요청은 정상적으로 `202`를 반환함
- Kafka CLI에서 해당 record의 offset이 증가함

## 8. 환경 종료

Spring Boot를 실행한 터미널에서 `Ctrl+C`로 애플리케이션을 종료한다.

Kafka 컨테이너를 잠시 멈추려면 다음을 사용한다.

```bash
docker compose stop
```

현재 Compose 구성에는 별도 volume이 없으므로 `docker compose down`으로 컨테이너를 제거하면 Kafka 컨테이너 내부에 저장된 학습용 record도 함께 사라질 수 있다. 실험 record를 보존하고 싶으면 `down` 대신 `stop`을 사용한다.

## 문제 해결

### `kafka-topics.sh: executable file not found`

Kafka CLI가 PATH에 등록되어 있지 않을 수 있다. 다음처럼 전체 경로를 사용한다.

```bash
/opt/kafka/bin/kafka-topics.sh
```

### Spring Boot가 Kafka에 연결하지 못함

호스트에서 실행하는 Spring Boot는 다음 주소를 사용해야 한다.

```text
localhost:29092
```

Docker 컨테이너 내부에서 실행하는 CLI는 다음 주소를 사용한다.

```text
kafka:9092
```

### `kafka-init`이 종료되어 있음

다음 상태라면 정상이다.

```text
kafka-init ... Exited (0)
```

topic 생성 명령이 끝나면 종료되는 초기화 컨테이너이므로 Kafka Broker인 `kafka`의 상태를 확인해야 한다.

## 최종 요약

Step 00은 `docker compose up -d`로 Kafka와 topic을 준비하고, `bootRun`으로 애플리케이션을 실행한 뒤 API·Producer callback·Consumer 로그·Kafka CLI를 순서대로 확인한다. `kafka-init`은 topic 생성 후 종료되는 정상적인 일회성 컨테이너다. Broker 장애 실험에서는 `docker compose stop kafka`와 `docker compose start kafka` 사이에 API 요청을 보내 Producer metadata 대기와 복구 후 record 저장을 관찰한다.
