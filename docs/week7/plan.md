# Week 7 구현 계획: ApplicationEvent 기반 정책 분리 (TDD & Tidy First)

본 문서는 `/docs/week2`의 설계 문서 변경사항(좋아요 집계 분리, 결제 알림 분리, 유저 행동 로깅 이원화)을 테스트 주도 개발(TDD) 및 Tidy First 원칙에 따라 구현하기 위한 계획서입니다. 모든 작업은 **Red -> Green -> Refactor** 사이클을 엄격히 준수하며, 기존 코드를 변경할 때는 **구조적 변경(Tidy)**을 먼저 수행한 뒤 **동작 변경**을 적용합니다.

---

## 1. 기반 인프라 구축 (Outbox Pattern)
이벤트 전달 보장(Guaranteed Delivery)을 위한 핵심 인프라인 `Outbox` 테이블과 관련 구조를 먼저 준비합니다.

*   **Task 1.1: `OutboxEvent` 엔티티 및 Repository 구현**
    *   **[Red]** `OutboxEvent` 엔티티 저장 및 상태(INIT, COMPLETED, FAILED) 조회가 성공하는 Repository 테스트 작성
    *   **[Green]** JPA Entity 및 JpaRepository 인터페이스 구현
    *   **[Refactor]** `EventType` Enum 도입 및 캡슐화 개선
*   **Task 1.2: Event Publisher 추상화 (Tidy First - 구조적 변경)**
    *   스프링의 `ApplicationEventPublisher`를 직접 참조하지 않고 내부 인터페이스(예: `EventPublisher`)로 감싸서 테스트 용이성(Mocking)을 확보합니다.
    *   기존 비즈니스 로직(OrderFacade, LikeFacade 등)에 `EventPublisher` 의존성을 주입(구조적 변경만 수행, 동작은 그대로 둠).

---

## 2. 좋아요(Likes) 집계 분리 (Eventual Consistency)
기존 단일 트랜잭션의 비관적 락 기반 집계를 이벤트 기반 비동기 집계로 변경합니다.

*   **Task 2.1: 좋아요 등록/취소 시 이벤트 발행**
    *   **[Red]** `LikeFacade`에서 좋아요 등록 시 `LikeCreatedEvent`가 발행되는지 확인하는 Mock 테스트 작성
    *   **[Green]** 기존 `Product.increaseLikeCount()` 호출 로직을 제거하고 `EventPublisher.publish()` 로직 추가
*   **Task 2.2: `LikeEventListener` (AFTER_COMMIT) 집계 트랜잭션**
    *   **[Red]** `LikeCreatedEvent` 수신 시 별도의 트랜잭션 내에서 `Product`에 비관적 락을 획득하고 `like_count`가 증가하는지 통합 테스트 작성
    *   **[Green]** `@TransactionalEventListener(phase = AFTER_COMMIT)` 및 `@Async`가 적용된 리스너 생성 및 집계 로직(비관적 락) 구현
*   **Task 2.3: 좋아요 집계 실패 시 Outbox 보상 기록**
    *   **[Red]** 리스너 내 집계 로직(DB 락 타임아웃 등 모의 예외) 실패 시 `OutboxEvent`가 `INIT` 상태로 저장되는지 검증
    *   **[Green]** 리스너 내 `try-catch`로 예외를 잡고 `OutboxRepository.save()` 수행

---

## 3. 유저 행동 로깅 이원화 (Fire-and-Forget vs Outbox)
중요도에 따라 로깅 전송 방식을 이원화합니다. 트랜잭션 결과와 무관하게 동작해야 하므로 `@EventListener`를 사용합니다.

*   **Task 3.1: 단순 행동(상품 조회 등) 로깅 (Fire-and-Forget)**
    *   **[Red]** 상품 상세 조회 시 `UserActionLogEvent`가 발행되고 리스너가 호출되는지 확인하는 테스트 (실패 시 예외가 비즈니스 로직에 전파되지 않는지 검증)
    *   **[Green]** `@EventListener`와 `@Async`를 적용한 리스너 생성. (단순 로그 출력 로직만 삽입)
*   **Task 3.2: 핵심 행동(주문/결제 시도) 로깅 (Outbox)**
    *   **[Red]** 주문 생성(OrderFacade) 호출 시 `UserActionLogEvent(중요도=HIGH)`가 발행되고, 리스너가 이를 수신하여 Outbox 테이블에 `INIT` 상태로 저장하는지 테스트
    *   **[Green]** 리스너 내부 분기문(이벤트의 중요도 확인)을 통해 HIGH 레벨인 경우 `OutboxRepository.save()` 호출 로직 구현

---

## 4. 결제 완료 알림 발송 분리 (Notification)
결제 커밋 이후 알림을 비동기로 쏘고, 실패 시 Outbox에 저장합니다.

*   **Task 4.1: 결제 성공 시 알림 이벤트 발행**
    *   **[Red]** `PaymentFacade`가 결제 승인을 DB에 커밋할 때 `PaymentCompletedEvent`가 발행되는지 테스트
    *   **[Green]** 결제 성공 상태로 업데이트된 후 `publishEvent()` 호출
*   **Task 4.2: 알림 리스너 및 실패 Outbox 저장**
    *   **[Red]** 이벤트 수신 후 외부 알림 API(Mock) 호출 검증. 호출 실패(Timeout) 시 Outbox 테이블에 내역이 저장되는지 통합 테스트
    *   **[Green]** `@TransactionalEventListener(AFTER_COMMIT)` 리스너 생성. `try-catch` 구현을 통해 실패 시 Outbox에 알림 재발송용 레코드 생성

---

## 5. Outbox 보정 스케줄러 구현 (Guaranteed Delivery)
저장된 Outbox 실패 내역을 배치로 읽어서 재처리합니다.

*   **Task 5.1: 스케줄러 재처리 로직 구현**
    *   **[Red]** DB에 수동으로 `INIT` 상태의 Outbox 데이터(좋아요, 알림, 중요 로깅 등)를 넣고, `Scheduler`의 재처리 메서드를 실행했을 때 상태가 `COMPLETED`로 변경되고 관련 로직(집계 갱신, 로그 발송)이 1회 호출되는지 테스트
    *   **[Green]** `@Scheduled` 메서드를 구현하여 `findByStatus('INIT')` 후 순차적으로 이벤트 종류(EventType)에 맞는 재처리 로직(Strategy 패턴 또는 단순 switch) 수행
    *   **[Refactor]** 이벤트 타입별 재처리 로직을 별도 컴포넌트(Strategy)로 추출하여 OCP 원칙 준수
