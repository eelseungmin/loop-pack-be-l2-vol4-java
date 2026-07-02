# Week 7 구현 및 수정 작업 계획 (TDD/Tidy First 기반)

이 문서는 7주 차 아키텍처 결정 사항(Kafka 기반 이벤트 분리, Outbox 패턴, 동시성 제어 등)을 시스템에 반영하기 위한 작업 계획입니다. 
Kent Beck의 TDD(Red-Green-Refactor)와 Tidy First 원칙에 따라 구조적 변경(Structural)과 행동 변경(Behavioral)을 분리하고, 실패하는 테스트를 먼저 작성하는 워크플로우를 따릅니다.

## 1. 결제 도메인 동시성 제어 및 보상 트랜잭션 비동기화

**목표:** 결제 콜백과 스케줄러 간의 상태 갱신 경합을 제어하고, 보상 트랜잭션 실패를 방지합니다.

- [ ] **결제 도메인 분산 락 동시성 제어 (Behavioral)**
  - 결제 콜백과 스케줄러가 동시 실행되는 시나리오에 대한 실패하는 테스트 작성 (Red)
  - `Redisson`을 활용한 분산 락(`payment:lock:{orderId}`) 기반의 동시성 갱신 방어 로직 구현 (Green)
- [ ] **보상 트랜잭션 비동기화 및 아웃박스 발행 (Behavioral)**
  - 기존 동기 방식 보상 로직(재고/쿠폰 복구) 인터페이스 분리 (Structural)
  - 보상 트랜잭션 실패 시나리오 및 이벤트 발행 누락에 대한 실패하는 테스트 작성 (Red)
  - 결제 실패 이벤트를 `Outbox` 테이블에 함께 저장하여 Kafka로 재처리 위임하도록 구현 (Green)
- [ ] **PG 결제 콜백 API 구현 (Behavioral)**
  - 결제 콜백 수신 및 정상 동작에 대한 실패하는 테스트 작성 (Red)
  - 콜백 수신 후 이벤트 발행 최소 코드 구현 (Green)

## 2. Transactional Outbox 및 Kafka Producer 파이프라인

**목표:** 메인 트랜잭션과 통신(Kafka)을 안전하게 분리하고 데이터 유실을 100% 차단합니다.

- [ ] **Outbox 엔티티 및 스키마 생성 (Structural)**
  - `OUTBOX_EVENTS` 테이블 (필드: id, aggregate_type, aggregate_id, payload, status 등) 및 리포지토리 구성
- [ ] **주문/결제 이벤트의 Outbox 동시 저장 (Behavioral)**
  - 핵심 트랜잭션 내 이벤트가 `INIT` 상태로 DB에 올바르게 저장되는지 확인하는 테스트 작성 (Red)
  - 이벤트 리스너(동기 방식)를 통해 DB에 `Outbox` 데이터를 삽입하는 로직 구현 (Green)
- [ ] **Kafka 비동기 전송 처리 (Behavioral)**
  - `AFTER_COMMIT` 시점에 `@Async`로 Kafka 전송이 이루어지는지 통합 테스트 작성 (Red)
  - TransactionalEventListener 설정 및 KafkaTemplate을 이용한 전송 로직, 성공 시 `PUBLISHED` 상태 갱신 로직 구현 (Green)
- [ ] **Outbox 폴링 스케줄러 구현 (Behavioral)**
  - 1~3초 주기로 `INIT` 상태의 이벤트를 카프카로 잘 쏘는지 검증하는 테스트 작성 (Red)
  - `@Scheduled`를 활용하여 미발송 데이터를 조회 및 발송, 그리고 재시도 로직 구현 (Green)
- [ ] **Kafka Producer 설정 적용 (Structural)**
  - `acks=all`, `enable.idempotence=true` 옵션 반영 및 파티션 키(aggregate_id) 매핑 설정

## 3. Kafka Consumer 및 멱등성(Exactly-Once) 보장

**목표:** Kafka 메시지를 안전하게 수신하고 이벤트의 중복 처리(At-Least-Once의 한계)를 완벽하게 방어합니다.

- [ ] **멱등성 방어 테이블 구성 (Structural)**
  - `EVENT_HANDLED` 테이블 (필드: event_id 등) 및 리포지토리 구성
- [ ] **메시지 수신 및 수동 커밋 처리 (Behavioral)**
  - Kafka 메시지 수신 실패 시 커밋되지 않고, 성공 시 수동 커밋되는 수신 테스트 작성 (Red)
  - Consumer의 `enable.auto.commit = false` 설정 및 `Acknowledgment.acknowledge()` 처리 (Green)
- [ ] **중복 메시지 방어 로직 (Behavioral)**
  - 동일한 `event_id` 메시지가 두 번 수신되었을 때 DB 수치가 1번만 오르는 것을 검증하는 멱등성 테스트 작성 (Red)
  - 메인 업데이트 트랜잭션 내부에서 `EVENT_HANDLED` 테이블의 고유 ID를 기록/검증하여 중복 처리 건너뛰기 구현 (Green)

## 4. 유저 행동 서버 레벨 로깅 및 부가 기능 (Eventual Consistency)

**목표:** 주문/결제 등 중요 데이터는 안전하게 로그를 남기고, 단순 조회의 로깅은 비동기 응답성 향상에 집중합니다.

- [ ] **중요 비즈니스(주문/결제) 실패 로깅 적용 (Behavioral)**
  - 결제 실패 시 이벤트 로깅 데이터가 유실되지 않고 Outbox 테이블에 들어가는지 검증하는 테스트 (Red)
  - 중요한 퍼널 로그에 대해 Outbox 패턴으로 발행하도록 적용 (Green)
- [ ] **단순 조회/좋아요 비동기 로깅 (Behavioral)**
  - 상품 단순 조회 시 에러가 나더라도 로깅은 `Fire & Forget` 처리되는 비동기 리스너 테스트 (Red)
  - `@Async` 기반 단순 이벤트 발행 및 로그 출력(또는 비동기 Kafka 발송) (Green)

---
### TDD/Tidy First 작업 수칙 요약
* **Red → Green → Refactor**: 모든 신규 구현은 가장 간단한 통합/단위 테스트(Red) 작성 후 구현(Green)합니다.
* **구조와 행동의 분리**: 단순 패키지 이동/클래스 분리(Structural)는 기능 추가(Behavioral) 커밋과 분리하여 선행합니다.
* 커밋 메시지 규칙 준수: `feat`, `fix`, `refactor` 등 적절한 Prefix를 활용해 작고 빈번한 커밋을 수행합니다.
