# Step 00 broker-down 실험

- 설정: Docker Compose의 단일 KRaft broker와 `order-events` topic을 사용한다.
- 행동: broker를 중지한 뒤 `POST /orders`를 호출한다.
- 예상: producer 전송 실패가 로그에 남기며, HTTP 202만으로 Kafka 발행 성공을 판단하지 않는다.
- 실제: broker 중지 직후 요청은 10초 내 응답하지 않았고 producer가 `localhost:29092` 재접속을 반복하며 `Connection ... could not be established` 경고를 남겼다.
- 결론: 현재 기본 producer의 metadata 조회는 동기 대기한다. broker가 정상일 때의 HTTP 202은 Kafka 기록 성공 증거가 아니며 callback과 CLI를 함께 확인해야 한다.
