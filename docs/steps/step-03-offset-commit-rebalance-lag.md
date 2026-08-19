# Step 03 — Offset, commit, rebalance, lag

## 사용자가 체감하는 변화

결제 처리가 한 번 끝났더라도 consumer가 죽는 시점에 따라 같은 주문이 다시 처리되거나, 잘못된 commit 순서에서는 처리되지 않은 이벤트가 건너뛸 수 있음을 본다.

## 목표와 완료 조건

- `enable-auto-commit=false`와 수동 acknowledgment를 사용한다.
- 업무 처리 성공 뒤 commit 전 종료 시 중복을 재현한다.
- commit 뒤 업무 처리 전 종료 시 유실 위험을 설명한다.
- consumer group lag를 계산하고 rebalance 로그를 해석한다.

## 이전 상태와 이번 변경

step-02의 `payment-service`를 유지한다. 가짜 결제 처리는 처음에는 로그 또는 메모리 상태 변경으로 충분하다. 외부 DB는 step-09 전까지 넣지 않는다.

## 핵심 이론

committed offset은 “다음에 읽을 위치”다. lag는 대략 `log end offset - committed offset`이며 partition별로 계산한다. 처리 후 commit은 중복 가능성을 만들고, commit 후 처리는 유실 가능성을 만든다. Kafka의 일반적인 소비 모델은 at-least-once다.

## 구현 순서

1. listener container를 manual ack로 설정한다.
2. `PaymentRequested` 처리 함수가 성공한 뒤에만 `acknowledge()`를 호출한다.
3. 테스트 전용 header 또는 orderId 규칙으로 “처리 후 commit 전 프로세스 종료”를 유발한다.
4. 재시작 뒤 동일 `eventId`가 다시 보이는지 로그로 확인한다.
5. 비교용으로 의도적으로 먼저 ack한 뒤 처리 전에 종료하는 경로를 만들고, 왜 위험한지 기록한다.
6. producer만 실행해 backlog를 만든 후 `kafka-consumer-groups.sh --describe`로 lag를 본다.
7. 인스턴스를 시작·종료해 revoke/assign 로그와 partition 재할당을 수집한다.

## 실행·검증

```bash
docker compose exec kafka kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group payment-service --describe
```

긴 처리로 `max.poll.interval.ms`를 넘기면 consumer가 group에서 제외돼 rebalance될 수 있다. 긴 작업은 단순히 poll thread에서 sleep하는 방식으로 해결하지 않는다.

## 실패 실험

- 처리 성공 직후 `Runtime.getRuntime().halt(...)` 또는 디버거 중단으로 commit 전 종료를 재현한다.
- consumer를 멈춘 뒤 주문을 20개 보내 lag가 쌓이는 것을 본다.
- 처리 시간을 의도적으로 늘려 rebalance를 관찰한다.

## 다음 단계로 넘기는 상태

중복은 정상적으로 발생할 수 있다는 전제가 생겼다. step-04에서는 이력 log와 최신 상태 조회용 log를 구분하고, step-10에서 업무 중복 제거를 구현한다.
