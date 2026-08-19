# Step 01 — 이벤트 계약과 JSON 직렬화

## 사용자가 체감하는 변화

주문 이벤트가 임시 JSON 문자열에서 버전과 식별자를 갖춘 계약으로 바뀐다. 소비자는 이벤트 종류와 버전을 보고 안전하게 해석하며, 호환되는 변경과 깨지는 변경의 차이를 직접 본다.

## 목표와 완료 조건

- 모든 이벤트가 `eventId`, `eventType`, `eventVersion`, `occurredAt`, `payload`를 가진다.
- `OrderCreated` v1과 하위 호환되는 v2를 소비한다.
- 필수 필드 삭제 또는 타입 변경이 consumer를 깨뜨리는 것을 재현한다.

## 이전 상태와 이번 변경

step-00의 `order-events`, 주문 API, producer/observer를 유지한다. `String` payload를 Java record 기반 event envelope로 바꾸고 JSON serializer/deserializer를 설정한다. `payment-events` 토픽은 지금 생성해도 되지만 결제 소비 흐름은 step-02에서 완성한다.

## 계약

```json
{
  "eventId": "7d1d1cb2-9b4d-4c67-b512-1e1d9f4c5290",
  "eventType": "OrderCreated",
  "eventVersion": 1,
  "occurredAt": "2026-08-19T10:00:00Z",
  "payload": {"orderId":"1001","amount":15000}
}
```

`eventId`는 전달 중복을 식별하기 위한 값이고, `eventVersion`은 스키마 호환성을 판단하기 위한 값이다. 둘은 대체 관계가 아니다.

## 구현 순서

1. `EventEnvelope<T>`와 `OrderCreatedPayload` record를 만든다.
2. producer에는 `JsonSerializer`, consumer에는 신뢰할 패키지를 제한한 `JsonDeserializer` 또는 Spring의 JSON message converter를 설정한다.
3. topic header에 `eventType`, `eventVersion`을 넣거나 envelope의 값을 기준으로 역직렬화 경로를 명확히 한다.
4. observer가 역직렬화된 envelope와 metadata를 로그로 남긴다.
5. v2에는 optional `couponCode`만 추가한다. consumer는 값이 없을 때 기본 처리한다.
6. 별도 실험 producer로 `amount`를 문자열로 바꾸거나 `orderId`를 제거한 잘못된 메시지를 보내 listener 실패를 관찰한다.

## 실행·검증

v1과 v2를 각각 발행한다. 이전 consumer가 v2의 optional 필드를 무시하고 처리하면 하위 호환이다. JSON은 읽힌다고 해서 의미가 호환되는 것이 아니다. 숫자 단위(원/센트), 금액 타입, 필수 여부도 계약이다.

## 실패 실험

- `amount: 15000`을 `amount: "15000"`으로 바꾼다.
- `orderId`를 제거한다.
- 새 필드를 optional이 아닌 필수로 가정하는 구 consumer를 둔다.

실패 record가 재전달되며 반복될 수 있으므로, 아직 retry/DLT를 구현하지 않았다는 사실을 기록한다.

## 다음 단계로 넘기는 상태

계약 있는 `OrderCreated`가 준비됐다. step-02에서 `order-events`를 3 partition으로 바꾸고, 결제 consumer group이 이를 받아 `PaymentRequested`를 `payment-events`에 발행한다.
