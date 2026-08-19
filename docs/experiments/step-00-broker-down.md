# Step 00 broker-down 실험

Kafka Broker가 중지된 상태에서 주문 API를 호출해 Producer 연결 실패, HTTP 응답 지연, Broker 복구 후 발행 결과를 확인한다.

## 설정

- Docker Compose의 단일 KRaft Broker
- topic: `order-events`
- partition: 1개
- replication factor: 1
- Spring Boot: `localhost:8080`
- Kafka bootstrap server: `localhost:29092`

## 실행 과정

1. Kafka와 topic을 기동한다.
2. 정상 상태에서 `POST /orders`를 호출해 기준 응답을 확인한다.
3. `docker compose stop kafka`로 Broker만 중지한다.
4. 같은 애플리케이션에 주문 API를 호출한다.
5. Kafka를 다시 기동하고 topic의 record와 Producer callback을 확인한다.

## 예상 결과

- 정상 상태에서는 HTTP `202 Accepted`가 반환된다.
- Broker 중지 상태에서는 Producer가 `localhost:29092`로 재접속을 시도한다.
- HTTP 요청이 즉시 성공하지 않으며, callback 성공 로그도 바로 남지 않는다.
- Broker 복구 후 Producer가 pending 요청을 처리할 수 있다.
- HTTP 응답 성공 여부만으로 Kafka record 저장 여부를 판단할 수 없다.

## 실제 결과

### 정상 상태 기준

```text
HTTP 202
응답 시간: 약 0.23초
Producer callback: key=step00-baseline, partition=0, offset=0
```

topic 상태도 다음과 같이 확인했다.

```text
PartitionCount: 1
ReplicationFactor: 1
Partition: 0
Leader: 1
Isr: 1
```

### Broker 중지 상태

Broker를 중지한 뒤 다음 요청을 보냈다.

```json
{"orderId":"step00-broker-down","amount":1000}
```

12초 timeout을 지정한 클라이언트의 결과는 다음과 같았다.

```text
curl: (28) Operation timed out after 12002 milliseconds with 0 bytes received
HTTP 000
TIME 12.002053s
```

Spring Boot 로그에는 다음과 같은 Producer 재접속 로그가 반복해서 남았다.

```text
Bootstrap broker localhost:29092 disconnected
Connection to node -1 (localhost/127.0.0.1:29092) could not be established
Node may not be available
Rebootstrapping with [localhost/127.0.0.1:29092]
```

Consumer도 Broker와 연결이 끊긴 뒤 재접속을 시도했다.

### Broker 복구 후

Broker를 다시 기동하자, 클라이언트는 이미 timeout됐지만 서버 내부에서 대기하던 Producer 요청이 계속 처리되었다.

```text
Producer callback: key=step00-broker-down, partition=0, offset=1
```

복구 후 새로 보낸 요청은 정상적으로 처리되었다.

```text
HTTP 202
응답 시간: 약 0.003초
Producer callback: key=step00-after-restart, partition=0, offset=2
```

Kafka CLI로 partition 0의 record를 확인한 결과는 다음과 같다.

```text
Partition:0 Offset:0 step00-baseline
Partition:0 Offset:1 step00-broker-down
Partition:0 Offset:2 step00-after-restart
```

## 결론

현재 Producer는 비동기 `CompletableFuture`를 사용하지만, `kafkaTemplate.send()`가 Future를 반환하기 전 metadata를 조회하는 과정에서 Broker 연결을 기다릴 수 있다. 따라서 Broker가 중지되면 HTTP 요청 thread가 지연될 수 있으며, client가 timeout된 뒤에도 서버 내부 Producer 작업이 계속되어 나중에 record가 저장될 수 있다.

`whenComplete()`는 Consumer의 업무 처리 완료가 아니라 Producer가 Broker 저장 결과를 받은 시점이다. HTTP 응답, Producer callback, Consumer 로그, Kafka CLI 결과는 서로 다른 관찰 지점이므로 각각 확인해야 한다.
