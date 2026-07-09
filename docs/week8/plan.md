# Week 8 Implementation Plan: 대기열 시스템 (Redis 기반)

본 문서는 TDD (Test-Driven Development) 원칙을 준수하여, Redis 기반 대기열 시스템을 구축하기 위한 계획서입니다. 모든 구현은 "실패하는 테스트 작성(Red) -> 최소한의 코드 구현(Green) -> 리팩토링(Refactor)" 사이클을 따릅니다.

## 1. 아키텍처 및 도메인 설계 방침
- **Interfaces Layer**: `QueueController`, 토큰 검증용 `TokenInterceptor`
- **Application Layer**: `QueueFacade` (도메인 서비스 조립), `QueueScheduler` (스케줄링)
- **Domain Layer**: `QueueService`, `TokenService` (Redis 기반 대기열 및 토큰 상태 관리 정책)
- **Infrastructure Layer**: `RedisQueueRepository`, `RedisTokenRepository` (Redis 연동 구현체)
- **에러 처리**: `CoreException`과 `ErrorType` 활용 (`UNAUTHORIZED`, `TOO_MANY_REQUESTS` 등)

## 2. 단계별 구현 계획

### Step 1: Redis 기반 대기열 구현
**목표**: 유저가 대기열에 진입하고 자신의 상태(순번)를 조회할 수 있게 한다. 중복 진입을 방지한다.

1. **[테스트] 대기열 진입 (Red)**
   - 대기열 진입 시 정상적으로 응답을 반환하는지 테스트.
   - 동일 유저(userId)가 여러 번 진입을 시도할 때 중복 진입이 되지 않고 기존 대기열 상태를 유지하는지 테스트.
2. **[구현] 대기열 진입 로직 (Green)**
   - `POST /api/v1/queue/enter` API 구현.
   - Redis의 `Sorted Set`을 사용하여 `score`를 진입 timestamp로 설정.
3. **[테스트] 대기 인원 및 순번 조회 (Red)**
   - 특정 유저의 순번(1-based)을 조회하는 테스트 작성 (`ZRANK` 활용).
   - 대기열의 전체 인원 수를 조회하는 테스트 작성 (`ZCARD` 활용).
4. **[구현] 조회 로직 (Green)**
   - `QueueService` 내 순번 조회 및 전체 대기 인원 조회 기능 구현.
5. **[리팩토링]** 
   - Redis 키 설계(Namespace 분리) 및 예외 처리 최적화.

### Step 2: 입장 토큰 & 스케줄러 (Batch Size: 21)
**목표**: 스케줄러가 대기열에서 N명씩 꺼내 Active 상태(토큰)로 전환하며, 토큰은 TTL을 갖는다. 결제/주문 API는 토큰을 검증한다.
*(산정 근거: K6 부하 테스트 기준 종합 병목 42.41 TPS * 1초 * 0.5(안전율) ≒ 21명/초)*

1. **[테스트] 스케줄러 토큰 발급 로직 (Red)**
   - 스케줄러 실행 시 `Waiting Queue`의 맨 앞 N명이 `Active Set`으로 이동하는지 테스트.
   - 발급된 토큰에 TTL(5분)이 정상 부여되는지 테스트.
2. **[구현] 스케줄러 (Green)**
   - `QueueScheduler` 구현 (`@Scheduled(fixedDelay = 1000)`).
   - 1초마다 대기열에서 21명 추출하여 토큰(Active) 발급.
3. **[테스트] 토큰 만료(TTL) 확인 (Red)**
   - TTL 초과 시 Redis에서 토큰이 자동 무효화(삭제)되는지 테스트.
4. **[구현] 주문 API 인터셉터 및 검증 (Green)**
   - `TokenInterceptor`를 구현하여 `/api/v1/orders` 접근 시 Active 토큰 검증 수행.
   - 주문/결제 완료 후 토큰을 삭제하는 로직 구현.
5. **[리팩토링]**
   - 인터셉터 등록 및 스케줄러 스레드 풀 격리.

### Step 3: 실시간 순번 조회 (Polling 최적화)
**목표**: 유저가 대기 중 현재 순번과 예상 대기 시간을 조회하고, 자기 차례가 되면 토큰을 반환받는다.

1. **[테스트] 예상 대기 시간 및 상태 조회 (Red)**
   - 대기 중인 유저는 (현재 순번 / 초당 처리량(21))으로 예상 대기 시간(초)을 반환받는지 테스트.
   - 이미 Active 상태가 된 유저가 조회 시 입장 토큰을 정상적으로 포함해 반환하는지 테스트.
2. **[구현] 순번 조회 API (Green)**
   - `GET /api/v1/queue/position` API 구현.
   - 응답 DTO에 현재 상태(WAITING/ACTIVE), 순번, 예상 대기 시간, 입장 토큰 포함.
3. **[테스트] 동시 진입 및 처리량 한계 (Red)**
   - 스케줄러 배치 크기 이상의 요청이 들어올 때 대기열 순서가 정확히 보장되는지 다중 스레드 환경에서 검증 (Concurrent Test).
4. **[구현 & 리팩토링]**
   - 테스트 통과 확인 후 병목 지점 Redis 연산 (Pipeline 또는 Lua Script 활용 고려) 최적화.

---

## 3. 검증 (Checklist)

- [ ] **동시 진입 테스트**: 수십 개의 스레드에서 동시에 대기열 진입 요청 시 `Sorted Set`을 통해 순서가 정확히 보장되는가?
- [ ] **토큰 만료 테스트**: 발급 후 TTL(5분)이 초과하면 토큰이 무효화되어 주문 API 접근이 차단되는가?
- [ ] **처리량 초과 테스트**: 초당 21명(N=21) 이상의 트래픽이 발생했을 때, 대기열이 정상적으로 유저를 수용하고 스케줄러가 N명 단위로 안정적 토큰 발급을 수행하는가?
- [ ] **산정 근거 문서화**: `docs/week8/decisions.md`에 기재된 부하 테스트 결과 및 N=21 산출 근거를 팀에 공유하고 유지하는가?
