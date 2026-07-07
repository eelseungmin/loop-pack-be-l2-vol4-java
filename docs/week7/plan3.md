# 선착순 쿠폰 발급 (비동기 처리) 구현 계획

이 계획은 Kent Beck의 테스트 주도 개발(TDD) 및 Tidy First 원칙에 따라 선착순 쿠폰 비동기 발급 기능을 구현하기 위해 작성되었습니다.

## 핵심 요구사항 (Step 3 반영)
1. **Redis 1차 고속 검증:** 유저 중복 발급 방지(`SADD`) 및 선착순 수량 체크(`INCR`)를 Redis를 통해 원자적으로 처리.
2. **비동기 메시지 발행:** 1차 검증을 통과한 요청만 Kafka로 '발급 요청 이벤트' 발행.
3. **폴링(Polling) API:** 클라이언트가 `requestId`를 통해 쿠폰 발급 상태(`IN_PROGRESS`, `SUCCESS`, `FAILED`)를 확인할 수 있는 API 제공.
4. **Kafka Consumer 락 제어:** 실제 발급 시 DB의 `COUPON_TEMPLATES` 테이블에 대해 비관적 락(Pessimistic Lock)을 획득하고 최종 수량을 검증하여 `COUPON_ISSUES`에 발급 내역 저장 및 Redis 상태 업데이트.

---

## 📅 구현 단계 (TDD 사이클: Red -> Green -> Refactor)

### Step 1. 도메인 및 DB 스키마 수정 (구조적 변경 - Tidy First)
- **목표:** ERD 변경 사항(`total_quantity`, `issued_quantity` 컬럼 추가)을 엔티티와 테이블에 반영.
- **TDD:**
  1. `CouponTemplate`에 신규 필드 추가에 따른 Repository 조회/저장 테스트 수정 (Red).
  2. 필드 추가 및 JPA 엔티티 매핑 (Green).
  3. `increaseIssuedQuantity()` 등의 도메인 로직 테스트 및 구현.
- **커밋:** `refactor:` 또는 `feat:` 규칙 준수.

### Step 2. Redis 1차 검증 컴포넌트 (`CouponIssueValidator`)
- **목표:** Redis 기반의 `SADD` 및 `INCR` 검증 로직 구현.
- **TDD:**
  1. **Red:** 동일 유저가 두 번 요청 시 두 번째는 실패(예외)하는지 테스트 코드 작성. 수량 초과 시 실패하는지 테스트.
  2. **Green:** `RedisTemplate` (또는 Lua Script)를 활용한 최소한의 검증 로직 구현.
  3. **Refactor:** 하드코딩된 Key 값이나 매직 넘버 상수화.

### Step 3. 상태 관리 및 폴링 API (요청 큐 생성)
- **목표:** `requestId` 기반으로 Redis에 상태(`IN_PROGRESS`)를 적재하고, 이를 조회할 수 있는 폴링 로직 구현.
- **TDD:**
  1. **Red:** `requestId`로 상태를 조회할 때 해당 상태 객체가 반환되는지 Controller/Facade 레벨의 테스트 작성.
  2. **Green:** `CouponFacade` 내 상태 저장/조회 로직 및 `GET /api/v1/coupons/requests/{requestId}` 엔드포인트 구현.
  3. **Refactor:** 상태 Enum(`CouponRequestStatus`) 정의 및 중복 제거.

### Step 4. Kafka Producer 및 발급 요청 API (`CouponIssueFacade` 업데이트)
- **목표:** 클라이언트 요청 시 1차 검증 -> 상태 IN_PROGRESS 적재 -> Kafka 발행 -> 202 Accepted 반환 흐름 완성.
- **TDD:**
  1. **Red:** 쿠폰 발급 요청 시 `CouponIssueValidator`를 거치고 `KafkaProducer`가 호출되는지(Mocking) 확인하는 단위/통합 테스트 작성.
  2. **Green:** `POST /api/v1/coupons/{couponId}/issue` API 연동 및 Facade 로직 구현.
  3. **Refactor:** 책임이 분리되도록 구조 재조정 (Tidy First 원칙에 따라 구조적 변경과 기능 변경 분리 커밋).

### Step 5. Kafka Consumer 및 실제 DB 발급 처리 (`CouponKafkaConsumer`)
- **목표:** 이벤트를 수신하여 DB 비관적 락을 걸고 최종 발급 후 Redis 상태(`SUCCESS`/`FAILED`) 갱신.
- **TDD:**
  1. **Red:** DB `COUPON_TEMPLATES` 비관적 락을 잡고 수량을 증가시키는 로직을 다수의 스레드에서 호출했을 때 초과 발급되지 않는지 동시성 테스트 작성.
  2. **Green:** `Repository`에 `@Lock(LockModeType.PESSIMISTIC_WRITE)` 쿼리 추가 및 도메인 로직 처리.
  3. **Red:** Consumer가 이벤트를 받아서 로직을 수행하고, 완료 후 Redis의 상태를 갱신하는지 통합 테스트 작성.
  4. **Green:** Consumer 클래스와 실제 처리 Facade 연결.
  5. **Refactor:** 에러 처리 및 트랜잭션 경계 명확화.

### Step 6. 전체 E2E 동시성 및 통합 검증
- **목표:** 실제 카프카, 레디스 환경을 띄우고 100명 한정 쿠폰에 1만 건의 요청이 들어왔을 때, 100장만 정확하게 발급되고 나머지는 실패하는지 검증.
- **TDD (Test First):**
  1. Testcontainers 기반 Kafka + Redis 환경 구성 테스트 작성.
  2. `CountDownLatch` 또는 `ExecutorService`를 활용해 API로 무작위 동시 요청 실행.
  3. 최종 `issued_quantity`가 100인지, 성공 응답(`SUCCESS`)을 받은 폴링 결과가 100건인지 단언(Assert).

---

## 🛠 코드 품질 및 커밋 가이드라인
- **작은 증분 유지:** 각 Step은 하나의 로직 단위를 의미하며, Step 내에서도 TDD 사이클에 맞춰 하나의 테스트 -> 하나의 구현 단위로 **작고 빈번한 커밋**을 진행합니다.
- **구조적 변경 선행 (Tidy First):** 파일 이동, 리팩토링, 메서드 분리 등의 구조적 변경은 행위(Behavior) 추가 전에 `refactor:`로 미리 분리하여 커밋합니다.
- **커밋 메시지 준수:** `<type>: <명령형 요약>` (괄호 메시지 제외, 50자 이내).
  - 예: `test: 쿠폰 수량 차감 비관적 락 동시성 테스트 작성`
  - 예: `feat: 선착순 쿠폰 비동기 발급 Consumer 구현`
