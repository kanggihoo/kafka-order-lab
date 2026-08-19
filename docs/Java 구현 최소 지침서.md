---
title: Java 구현 최소 지침서
tags:
  - java
  - spring-boot
  - learning
status: active
---

# Java 구현 최소 지침서

이 프로젝트의 코드는 역할을 분리하고, 읽기 쉬우며, Step에서 검증할 동작만 구현한다.

## 적용 범위

모든 Java 코드에 적용한다. Step별 Kafka 설정, 재시도 정책, DB 설계처럼 학습 주제에 따라 달라지는 내용은 각 Step 문서에서 결정한다.

## 필수 규칙

1. **계층의 역할을 분리한다.**
   - controller는 HTTP 요청 검증과 응답만 담당한다.
   - service는 유스케이스와 비즈니스 로직을 담당한다.
   - Kafka consumer, repository 같은 외부 연동 코드는 controller에서 직접 사용하지 않는다.

2. **전달용 데이터는 DTO로 분리한다.**
   - 요청·응답 DTO, Kafka 이벤트 DTO, Entity는 서로 분리한다.
   - 단순 데이터를 표현하는 DTO와 이벤트는 `record`를 우선 사용한다.
   - Entity를 API 응답이나 Kafka 메시지로 직접 사용하지 않는다.

3. **하드코딩하지 않는다.**
   - topic, consumer group, broker 주소, 포트처럼 환경별로 달라지는 값은 `application.yml` 또는 환경 변수로 관리한다.
   - 반복해서 쓰는 값은 의미 있는 상수로 분리한다.
   - 비즈니스 의미가 있는 문자열은 코드 곳곳에 중복해서 작성하지 않는다.

4. **입력은 경계에서 검증한다.**
   - HTTP 요청 DTO에는 필요한 Bean Validation을 선언한다.
   - controller에서 유효하지 않은 요청을 거르고, 검증 실패가 service까지 전달되지 않게 한다.
   - Kafka 메시지는 신뢰하지 않고, 해당 Step에서 필요한 계약 검증과 실패 처리를 둔다.

5. **의존성은 생성자로 주입한다.**
   - 의존 객체는 생성자로 받고 `final` 필드로 보관한다.
   - 필드 주입은 사용하지 않는다.
   - 다른 package에서 사용하지 않는 타입은 불필요하게 `public`으로 공개하지 않는다.

6. **메서드는 하나의 역할에 집중한다.**
   - 메서드 이름만으로 행동과 결과를 알 수 있게 작성한다.
   - controller, service, consumer가 서로의 역할을 대신하지 않게 한다.
   - 긴 메서드는 검증, 변환, 저장·발행처럼 의미 있는 단위로 나눈다.

7. **예외와 로그를 숨기지 않는다.**
   - controller에 `try-catch`를 작성하지 않고 `@RestControllerAdvice`에서 HTTP 예외 응답을 일관되게 처리한다.
   - service는 예상 가능한 예외를 의미 있는 custom exception으로 전환해 전달한다.
   - 클라이언트에는 stack trace나 내부 시스템 정보를 노출하지 않고, 서버 로그에만 기록한다.

8. **API 응답 형식을 통일한다.**
   - 성공과 실패 응답은 공통 응답 DTO로 반환한다.
   - 실패 응답에는 HTTP status, message, error code, field 오류(해당 시), timestamp, path를 포함한다.
   - error code와 HTTP status는 분리한다. HTTP status는 통신 결과를, error code는 애플리케이션 오류 종류를 표현한다.
   - Validation, 비즈니스, 인증·인가 예외도 같은 오류 응답 형식을 사용한다.

   | 구분 | 최소 필드 |
   |---|---|
   | 성공 | `success`, `status`, `message`, `data`, `timestamp` |
   | 실패 | `success`, `status`, `message`, `errorCode`, `errors`, `timestamp`, `path` |

9. **로거를 사용한다.**
   - `System.out.println` 대신 `@Slf4j`를 사용한다.
   - 예외 로그는 예외 객체를 마지막 인자로 전달해 stack trace를 남긴다. 예: `log.error("order processing failed: orderId={}", orderId, e);`
   - Kafka 처리 시에는 원인 분석에 필요한 key, topic, partition, offset을 로그에 남긴다.
   - 비밀번호, 토큰, 개인정보, 전체 payload처럼 민감할 수 있는 값은 로그에 남기지 않는다.

10. **주석과 테스트로 의도를 남긴다.**
   - public 클래스와 메서드에는 JavaDoc 형식으로 역할, 인자, 반환값 또는 예외를 설명한다. 코드만으로 분명한 내부 구현에는 불필요한 주석을 추가하지 않는다.
   - 테스트 메서드에는 `@DisplayName`으로 검증하는 동작을 한국어 문장으로 작성한다.

11. **변경한 동작은 검증한다.**
   - service 로직은 단위 테스트로, HTTP 요청·응답은 MVC 테스트로 확인한다.
   - 실제 Kafka 동작이 핵심인 Step은 통합 테스트 또는 CLI 실험으로 확인한다.
   - Step 완료 시 실험의 설정, 행동, 예상, 실제 결과, 결론을 `docs/experiments/`에 기록한다.

12. **필요한 만큼만 구현한다.**
   - 현재 Step의 학습 목표와 관계없는 의존성, 추상화, 인프라는 추가하지 않는다.
   - 공통화는 실제로 두 곳 이상에서 같은 문제가 반복될 때 검토한다.

## 변경 전 확인 목록

- [ ] controller, service, DTO의 역할이 섞이지 않았는가?
- [ ] 요청·이벤트·Entity를 목적에 맞는 별도 타입으로 표현했는가?
- [ ] 환경별 값과 반복 값이 하드코딩되어 있지 않은가?
- [ ] 전역 예외 처리와 공통 성공·오류 응답 형식을 지켰는가?
- [ ] 로그만으로 Kafka 처리 위치와 실패 원인을 추적할 수 있는가?
- [ ] public API에 JavaDoc을, 테스트에 `@DisplayName`을 작성했는가?
- [ ] 변경한 동작을 테스트 또는 실험으로 확인했는가?
- [ ] 이번 Step에 필요 없는 코드를 추가하지 않았는가?

## 최종 요약

controller, service, DTO의 역할을 나누고 단순 데이터에는 `record`를 사용한다. 예외는 전역에서 일관된 형식으로 응답하고, 내부 정보는 서버 로그에만 남긴다. 환경별 값은 하드코딩하지 않으며, `@Slf4j`, JavaDoc, `@DisplayName`으로 코드의 의도를 남긴다. 구현 범위는 현재 Step에 필요한 수준으로 제한하고, 변경 결과는 테스트와 실험 기록으로 확인한다.
