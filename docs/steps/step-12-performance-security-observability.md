# Step 12 — 성능, 보안, 관측성 선택 확장

## 사용자가 체감하는 변화

“Kafka를 붙였다”에서 끝나지 않고, 현재 주문 흐름에서 실제 병목·위험·장애 신호 하나를 측정해 개선한다. 이 단계는 모든 항목을 구현하는 체크리스트가 아니라 한 가지를 깊게 검증하는 선택 단계다.

## 목표와 완료 조건

- 아래 세 트랙 중 하나를 선택한다.
- 선택 전 기준값과 변경 후 결과를 같은 조건으로 비교한다.
- 결과와 한계를 `docs/experiments/step-12-<track>.md`에 기록한다.

## 이전 상태와 이번 변경

step-11까지의 주문/결제 이벤트, PostgreSQL Outbox/Inbox, 3-node cluster, 관측 절차를 유지한다. 선택하지 않은 트랙의 설정을 무리하게 추가하지 않는다.

## 트랙 A — 성능과 backpressure

`linger.ms`, batch size, compression, listener concurrency 중 하나를 바꾼다. 주문 N건을 고정된 payload와 key 분포로 발행하고, 처리 시간, producer batch 지표, partition별 lag, 오류율을 비교한다. listener 처리 시간이 길어지면 `max.poll.interval.ms`와 concurrency의 관계를 함께 측정한다. 처리량만 높이고 p99 지연 또는 hot partition을 숨기지 않는다.

## 트랙 B — 보안

SASL/SCRAM 인증과 최소 ACL을 적용한다. application principal에는 필요한 topic read/write와 consumer group 권한만 준다. 허가된 producer/consumer가 동작하고, 권한 없는 principal의 publish/consume이 실패하는 두 가지를 검증한다. TLS는 인증서 발급·신뢰 체인이 준비될 때 추가하며, 단순 로컬 plaintext 실습과 혼동하지 않는다.

## 트랙 C — 관측성

broker/controller 상태, partition leader/ISR, consumer lag, Outbox NEW/SENT 수, Inbox 중복 건수, DLT 적재 건수를 하나의 대시보드 또는 정기 점검 화면으로 모은다. 단순 Kafka UI 설치가 목적이 아니라, 각 지표가 어떤 장애 가설을 검증하는지 표시한다. 예: lag 상승은 consumer 중단/처리 지연, ISR 감소는 복제 내구성 저하, DLT 증가는 처리 불가 이벤트를 뜻한다.

## 실행·검증

선택한 트랙마다 다음 형식으로 기록한다.

```md
## 실험 목적
## 기준 설정과 결과
## 변경 설정과 결과
## 관찰 지표와 수집 방법
## 예상과 실제의 차이
## 결론: 유지/되돌림/추가 조사
```

## 실패 실험

성능은 consumer 중단으로 lag를 만든 뒤 복구 시간을 측정한다. 보안은 권한 없는 계정으로 publish를 시도한다. 관측성은 broker 중단 또는 DLT 유발 주문으로 지표가 실제로 변하는지 확인한다.

## 학습 완료 상태

이제 다음 질문에 코드와 관찰 결과로 답할 수 있어야 한다.

- ISR가 줄어든 환경에서 `acks=all`과 `min.insync.replicas`는 어떤 전송을 실패시키는가?
- Outbox relay가 send 성공 후 중단되면 왜 중복 발행될 수 있고, Inbox는 무엇을 막는가?
- consumer가 DB 처리 뒤 offset commit 전에 종료되면 왜 같은 event가 재전달되며, 업무 효과를 한 번으로 제한하려면 무엇이 필요한가?


## 코드 작성 규칙

- Java 클래스, record, public/protected 메서드에는 한글 Javadoc을 작성한다.
- Javadoc에는 코드의 의도와 필요한 경우 입력값, 반환값, 예외, Kafka 발행/소비 부작용을 기록한다.
- 구현이 자명한 private 코드에는 불필요한 Javadoc을 추가하지 않는다.
