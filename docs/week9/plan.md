# Week 9 Implementation Plan: 실시간 상품 랭킹

본 문서는 `/docs/week2` 설계 변경과 `/docs/week9/decisions.md`의 결정사항을 구현하기 위한 계획서입니다. 모든 구현은 TDD 원칙에 따라 **Red -> Green -> Refactor** 순서로 진행하며, 구조 변경과 행동 변경은 Tidy First 원칙에 따라 분리합니다.

## 1. 목표와 범위

### 목표
- Kafka 상품 이벤트를 `MetricsKafkaConsumer`와 `RankingKafkaConsumer`가 서로 다른 Consumer Group으로 독립 소비한다.
- `MetricsKafkaConsumer`는 기존 `product_metrics` 누적 집계를 갱신한다.
- `RankingKafkaConsumer`는 같은 이벤트를 Redis ZSET `ranking:all:{yyyyMMdd}`에 반영해 일간 실시간 랭킹을 만든다.
- Redis Set `ranking:handled:{yyyyMMdd}`로 `eventId` 기준 중복 가산을 방지한다.
- `GET /api/v1/rankings`로 일자별 인기상품 랭킹을 조회한다.
- 상품 상세 조회 응답에 오늘 기준 랭킹 정보를 포함한다.

### 범위
- `modules:event-contract`: `OUTBOX_EVENTS` 재빌드 조회에 사용할 읽기 전용 이벤트 로그 계약
- `modules:ranking-contract`: 랭킹 표준 이벤트, 이벤트 타입, 점수 정책, Redis Key/date 정책
- `apps:commerce-streamer`: `RankingKafkaConsumer`, 점수 계산 정책, Redis 랭킹 저장소, 멱등성 처리, 재빌드 Job
- `apps:commerce-api`: Ranking API, 상품 상세 응답 확장, 상품 정보 aggregation
- `docs/week9`: 구현 계획 및 검증 기준

### 비범위
- Redis ZSET을 RDB 테이블로 모델링하지 않는다.
- Kafka 배치 리스너는 기본 구현 범위가 아니라 선택적 최적화로 둔다.

## 2. 확정 설계

- 랭킹 날짜는 **이벤트 발생 시각** 기준으로 계산한다.
- 랭킹 ZSET Key는 `ranking:all:{yyyyMMdd}`이다.
- Redis 멱등성 Set Key는 `ranking:handled:{yyyyMMdd}`이다.
- 두 Key의 TTL은 모두 2일이다.
- 점수 계산식은 다음과 같다.
  - 조회: `0.1 * 1`
  - 좋아요: `0.2 * 1`
  - 주문: `0.6 * log(price * amount + 1)`
- `MetricsKafkaConsumer`와 `RankingKafkaConsumer`는 서로 다른 Consumer Group으로 같은 이벤트를 독립 소비한다.
- `MetricsKafkaConsumer`는 `product_metrics`만 갱신하고, `RankingKafkaConsumer`는 Redis 랭킹만 갱신한다.
- `product_metrics`와 Redis 랭킹은 독립 Projection이며, Consumer lag/장애 동안 일시적으로 다를 수 있다.
- `RankingKafkaConsumer`는 Redis 반영 성공 후에만 Kafka offset을 커밋한다.
- `modules/ranking-contract`는 `ProductRankingEvent`, `RankingEventType`, `RankingScorePolicy`, `RankingKeyPolicy`를 제공한다.
- `commerce-api`와 `commerce-streamer`는 `modules/ranking-contract`를 의존해 같은 ranking key/date/score 정책을 사용한다.
- `modules/event-contract`는 읽기 전용 `OutboxEventLog(id, eventType, status, payload, createdAt)` 계약을 제공한다. JPA Entity는 공용화하지 않는다.
- Outbox의 랭킹 표준 이벤트는 `PRODUCT_RANKING_EVENT` 단일 `event_type`으로 저장하고, 실제 행위는 payload의 `rankingEventType`으로 구분한다.
- DB raw payload에는 `eventId`를 저장하지 않는다. Kafka Relay와 재빌드 조회 구현이 `OUTBOX_EVENTS.id`를 `ProductRankingEvent.eventId`로 주입한다.
- 상품 상세 조회의 랭킹은 항상 서버의 오늘 날짜 기준이며, 랭킹에 없으면 `null`이다.
- 상품 논리 삭제 시 `rankingEventType=PRODUCT_DELETED`인 `PRODUCT_RANKING_EVENT` Outbox 이벤트를 발행하고, `RankingKafkaConsumer`가 최근 TTL 범위의 Redis ZSET에서 해당 상품을 제거한다.
- 랭킹 API 조회 시점에 남아 있는 삭제 상품은 2차 방어로 제외한다.
- Redis 데이터 유실 시 `commerce-streamer`가 `OUTBOX_EVENTS`에서 `PRODUCT_RANKING_EVENT`를 조회해 오늘/전일 랭킹을 `ranking:rebuild:*` 임시 Key에 재계산한 뒤 운영 Key로 교체한다.
- 재빌드 조회 대상 상태는 `INIT`, `COMPLETED`이며 `FAILED`는 제외한다.
- 재빌드 후보는 `createdAt` 기준 최근 3일 버퍼로 조회하고, 실제 반영 날짜는 payload의 `occurredAt` 기준으로 계산한다.
- 운영 Key 교체 중 `RankingKafkaConsumer`와의 충돌 방지는 후속 운영 절차로 남긴다. Consumer 일시 중단 또는 기준 시각 이후 이벤트 replay 중 하나를 선택해야 한다.

- 자정 직후 랭킹 콜드 스타트 완화를 위해 별도 `RankingCarryOverJob`을 둔다.
- `RankingCarryOverJob`은 00:10 이전에 전일 `ranking:all:{yesterday}` Top 1,000을 읽어 오늘 `ranking:all:{today}`에 `yesterdayScore * 0.1`로 합산한다.
- carry over 중복 실행 방지는 `ranking:carry-over:done:{yyyyMMdd}` Key로 처리하고 TTL은 2일로 둔다.
- carry over는 `ranking:handled:{yyyyMMdd}`를 수정하지 않는다. handled Set은 Kafka 이벤트 중복 방지 전용이다.
- 전일 랭킹 Key가 없거나 비어 있으면 점수 적재 없이 done key만 기록한다.
- carry over 전 Top 1,000 상품을 DB 조회해 삭제/미존재 상품은 제외한다. 삭제/미존재 제외는 정상 처리하고, DB 인프라 예외는 carry over 실패로 처리한다.
- carry over 실패와 무관하게 `RankingKafkaConsumer`의 실시간 랭킹 적재는 계속한다.
- carry over 검증은 단위/통합 테스트를 기본 완료 기준으로 삼고, E2E 검증은 선택 확장 항목으로 둔다.

## 3. 단계별 구현 계획

### Step 1. Ranking Contract 모듈 작성

**목표:** 이벤트 타입, 표준 이벤트 계약, 점수 정책, Redis Key/date 정책을 `modules:ranking-contract`의 순수 로직으로 분리한다.

1. **Red**
   - `ProductRankingEvent`, `RankingEventType`, `RankingKeyPolicy` 테스트를 작성한다.
   - `RankingScorePolicyTest` 작성.
   - 조회 이벤트는 `0.1`, 좋아요 이벤트는 `0.2`를 반환하는지 검증한다.
   - 주문 이벤트는 `0.6 * log(price * amount + 1)`을 반환하는지 검증한다.
   - 이벤트 발생 시각이 `2026-07-14T23:59:59`이면 dateKey가 `20260714`인지 검증한다.
   - `ranking:all:{yyyyMMdd}`, `ranking:handled:{yyyyMMdd}`, `ranking:rebuild:all:{yyyyMMdd}` Key가 같은 정책에서 계산되는지 검증한다.
2. **Green**
   - `modules:ranking-contract`를 추가한다.
   - `ProductRankingEvent`, `RankingEventType`, `RankingScorePolicy`, `RankingKeyPolicy`를 최소 구현한다.
   - 이벤트 타입별 Weight는 상수로 둔다.
3. **Refactor**
   - 이벤트 타입 문자열, Weight, date formatter를 명확한 이름으로 정리한다.
   - 정책 클래스는 Redis, Kafka, DB에 의존하지 않게 유지한다.

**검증:** `./gradlew :modules:ranking-contract:test`

### Step 2. Redis 랭킹 저장소 구현

**목표:** Redis ZSET과 handled Set을 다루는 저장소를 만든다.

1. **Red**
   - `RankingRedisRepositoryTest` 작성.
   - `ranking:handled:{yyyyMMdd}`에 `eventId`가 최초 저장될 때만 true를 반환하는지 검증한다.
   - 최초 처리 이벤트만 `ranking:all:{yyyyMMdd}` 점수를 증가시키는지 검증한다.
   - ZSET과 handled Set에 모두 2일 TTL이 설정되는지 검증한다.
2. **Green**
   - `RankingRedisRepository` 인터페이스와 Redis 구현체를 추가한다.
   - Redis Key 문자열은 `RankingKeyPolicy`를 통해 계산한다.
   - `SADD`, `ZINCRBY`, `EXPIRE`를 사용해 최소 구현한다.
3. **Refactor**
   - 중복 가산 방지를 위해 `SADD + ZINCRBY + EXPIRE`를 Lua Script로 원자화할지 검토한다.
   - 단건 구현이 통과한 뒤에만 Lua Script로 구조 개선한다.

**검증:** `./gradlew :apps:commerce-streamer:test`

### Step 3. RankingKafkaConsumer 신규 구현

**목표:** `RankingKafkaConsumer`가 `MetricsKafkaConsumer`와 독립 Consumer Group으로 같은 이벤트를 소비하고 Redis 랭킹만 갱신하도록 한다.

1. **Red**
   - `RankingKafkaConsumerTest`를 작성한다.
   - 조회/좋아요/주문 이벤트 수신 시 `RankingRedisRepository`가 호출되는지 검증한다.
   - `MetricsUpdateService`나 `product_metrics` 저장소를 호출하지 않는지 검증한다.
   - 이미 처리된 `eventId`는 Redis 랭킹이 중복 처리되지 않는지 검증한다.
   - 상품 삭제 이벤트 수신 시 최근 TTL 범위의 랭킹 ZSET에서 `ZREM`이 호출되는지 검증한다.
   - Redis 랭킹 반영 실패 시 수동 Ack가 수행되지 않는지 검증한다.
2. **Green**
   - `RankingKafkaConsumer`를 추가하고 `MetricsKafkaConsumer`와 다른 Consumer Group을 사용하도록 설정한다.
   - Kafka payload는 `modules:ranking-contract`의 `ProductRankingEvent`로 역직렬화한다.
   - 조회/좋아요/주문 이벤트는 `ranking:handled:{yyyyMMdd}` 확인 후 `ZINCRBY`를 수행한다.
   - 상품 삭제 이벤트는 별도 handled Set 없이 최근 TTL 범위의 `ranking:all:*`에서 `ZREM`을 수행한다.
   - Redis 반영이 성공한 뒤에만 Ack한다.
3. **Refactor**
   - Consumer 내부의 이벤트 파싱, 점수 반영, 삭제 반영을 작은 메서드로 분리한다.
   - 구조 변경 커밋과 동작 변경 커밋을 분리한다.

**검증:** `./gradlew :apps:commerce-streamer:test`

### Step 4. Ranking 조회 Application/API 추가

**목표:** Redis 랭킹 후보를 읽고 상품 정보를 조합해 랭킹 페이지를 반환한다.

1. **Red**
   - `RankingFacadeTest` 작성.
   - Redis에서 읽은 `productId` 목록을 상품 정보와 조합해 순위, 점수, 상품명, 브랜드명, 가격을 반환하는지 검증한다.
   - 삭제 이벤트 처리 지연 등으로 ZSET에 남아 있는 논리 삭제 상품은 응답에서 제외되는지 검증한다.
   - `date` 미입력 시 서버 오늘 날짜를 사용하는지 검증한다.
2. **Green**
   - `RankingFacade`, `RankingInfo`, `RankingRepository` 또는 API용 DTO를 추가한다.
   - `GET /api/v1/rankings` Controller를 추가한다.
   - 페이징 정책은 기존 프로젝트 방식을 따른다.
3. **Refactor**
   - API DTO와 Application DTO를 분리한다.
   - 상품 조회 aggregation 로직이 과도하게 커지면 private 메서드 수준으로만 정리한다.

**검증:** `./gradlew :apps:commerce-api:test`

### Step 5. 상품 삭제 시 랭킹 ZSET 정리

**목표:** 상품 논리 삭제 시 Outbox 이벤트를 남기고, `RankingKafkaConsumer`가 해당 이벤트를 소비해 최근 TTL 범위의 랭킹 ZSET에서 상품을 제거한다.

1. **Red**
   - `ProductAdminFacadeTest`에 상품 삭제 시 `rankingEventType=PRODUCT_DELETED`인 `PRODUCT_RANKING_EVENT` Outbox 이벤트가 저장되는지 테스트를 추가한다.
   - `RankingKafkaConsumerTest`에 상품 삭제 이벤트 수신 시 `ranking:all:{today}`, `ranking:all:{yesterday}`에서 해당 `productId`가 제거되는지 테스트를 추가한다.
   - 삭제된 상품이 ZSET에서 제거된 뒤 랭킹 API 다음 페이지와 중복 노출되지 않는지 검증한다.
   - Outbox Relay가 `PRODUCT_RANKING_EVENT` 발행 시 `OUTBOX_EVENTS.id`를 payload의 `eventId`로 주입하는지 검증한다.
2. **Green**
   - `ProductAdminFacade.deleteProduct` 흐름에서 상품 논리 삭제와 함께 `PRODUCT_RANKING_EVENT` Outbox 이벤트를 저장한다.
   - DB raw payload에는 `eventId`를 넣지 않고, payload 내부 `rankingEventType`을 `PRODUCT_DELETED`로 둔다.
   - Outbox Relay가 `PRODUCT_RANKING_EVENT` 발행 시 `OUTBOX_EVENTS.id`를 payload의 `eventId`로 주입한다.
   - `RankingRedisRepository.removeProductFromRecentRankings(productId)`를 구현한다.
   - `RankingKafkaConsumer`가 상품 삭제 이벤트를 소비해 최근 2일 랭킹 Key에 대해 `ZREM`을 수행한다.
3. **Refactor**
   - TTL 기간이 바뀌어도 제거 대상 Key 계산을 한 곳에서 관리하도록 정리한다.
   - `PRODUCT_RANKING_EVENT` 외의 기존 Outbox 이벤트는 payload를 변형하지 않도록 분기 범위를 좁힌다.
   - Redis 정리 실패 시 `RankingKafkaConsumer`가 Ack하지 않고 재처리한다는 정책을 테스트와 주석으로 명확히 남긴다.

**검증:** `./gradlew :apps:commerce-api:test`

### Step 6. 상품 상세 조회에 오늘 랭킹 정보 추가

**목표:** 상품 상세 응답에 오늘 기준 랭킹 정보를 포함한다.

1. **Red**
   - `ProductFacadeTest` 또는 `ProductV1ControllerTest`에 상품 상세 랭킹 응답 테스트를 추가한다.
   - 오늘 랭킹에 상품이 있으면 `rank`, `score`, `date`가 내려오는지 검증한다.
   - 오늘 랭킹에 상품이 없으면 `ranking`이 `null`인지 검증한다.
2. **Green**
   - `ProductInfo` 또는 상품 상세 DTO에 랭킹 필드를 추가한다.
   - `ProductFacade.retrieveProduct` 흐름에서 `RankingRedisRepository`를 조회한다.
3. **Refactor**
   - 기존 상품 상세 조회 캐시와 충돌하지 않는지 확인한다.
   - 랭킹이 실시간 값이면 상품 상세 캐시에 포함하지 않을지, 캐시 TTL 내 지연을 허용할지 별도 주석으로 의도를 남긴다.

**검증:** `./gradlew :apps:commerce-api:test`

### Step 7. Outbox/Event 로그 기반 Redis 랭킹 재빌드

**목표:** Redis 랭킹 데이터가 유실된 경우 `OUTBOX_EVENTS`에 남은 랭킹 표준 이벤트를 읽어 오늘/전일 랭킹을 재계산할 수 있도록 한다.

> 범위 조정: 기존 C 결정의 “실제 `OUTBOX_EVENTS` 조회 구현 보류”를 철회한다. 이번 단계에서는 `modules:event-contract`를 추가하고, `commerce-streamer`에서 `OUTBOX_EVENTS` 조회 구현까지 포함한다. 단, 운영 Key 교체 중 실시간 Consumer와의 충돌 방지 절차는 후속 운영 의사결정으로 남긴다.

1. **Red**
   - `OutboxEventLog` 계약 테스트를 작성한다.
   - `RankingRebuildEventRepositoryTest`를 작성한다.
   - `PRODUCT_RANKING_EVENT` 중 `INIT`, `COMPLETED` 상태만 조회하고 `FAILED`는 제외하는지 검증한다.
   - `createdAt` 기준 최근 3일 버퍼로 후보를 조회하는지 검증한다.
   - 조회한 `OUTBOX_EVENTS.id`를 `ProductRankingEvent.eventId`로 주입하는지 검증한다.
   - `RankingRebuildJobTest`를 작성한다.
   - 오늘/전일 범위의 조회/좋아요/주문 이벤트를 읽어 `ranking:rebuild:all:{yyyyMMdd}`에 점수가 재계산되는지 검증한다.
   - 상품 삭제 이벤트가 재빌드 결과에서 해당 상품을 제거하는지 검증한다.
   - 재빌드 완료 후 `ranking:rebuild:*` 임시 Key가 운영 Key(`ranking:*`)로 교체되는지 검증한다.
2. **Green**
   - `modules:event-contract`를 추가하고 `OutboxEventLog`를 구현한다.
   - `RankingRebuildJob`을 구현한다.
   - `RankingRebuildEventRepository` 구현체가 `OUTBOX_EVENTS`에서 `PRODUCT_RANKING_EVENT`를 조회한다.
   - 조회 조건은 `status in (INIT, COMPLETED)`, `createdAt >= 기준일 - 3일`로 둔다.
   - raw payload에는 `eventId`가 없으므로 `OUTBOX_EVENTS.id`를 `ProductRankingEvent.eventId`로 주입한다.
   - payload의 `occurredAt`이 오늘/전일인 이벤트만 재빌드에 반영한다.
   - `RankingScorePolicy`를 재사용해 점수를 계산하고 Redis 임시 Key에 적재한다.
   - 재빌드 완료 후 임시 Key를 운영 Key로 교체한다.
3. **Refactor**
   - 재빌드 대상 기간 계산(today/yesterday)을 설정 값 또는 정책 클래스로 분리한다.
   - `OUTBOX_EVENTS` 조회용 JPA Entity는 `commerce-streamer` 인프라에 두고, `modules:event-contract`에는 읽기 전용 계약만 남긴다.
   - 운영 Key 교체 중 Consumer 충돌 방지 절차는 후속 의사결정으로 남긴다.

**검증:** `./gradlew :apps:commerce-streamer:test`

### Step 8. Score Carry Over 기반 랭킹 콜드 스타트 완화

**목표:** 자정 직후 오늘 랭킹이 비어 있거나 부족한 문제를 줄이기 위해 전일 Top 1,000 랭킹 점수의 10%를 오늘 랭킹 초기 점수로 적재한다.

1. **Red**
   - `RankingKeyPolicyTest`에 `ranking:carry-over:done:{yyyyMMdd}` Key 계산 테스트를 추가한다.
   - `RankingCarryOverJobTest`를 작성한다.
   - 전일 Top 1,000만 조회하고 `yesterdayScore * 0.1`을 오늘 `ranking:all:{today}`에 합산하는지 검증한다.
   - carry over 완료 후 오늘 랭킹 ZSET과 done key에 2일 TTL이 설정되는지 검증한다.
   - done key가 이미 있으면 중복 실행하지 않는지 검증한다.
   - 00:10 이후 실행 시 skip하는지 검증한다.
   - 전일 랭킹 Key가 없거나 비어 있으면 점수 적재 없이 done key만 기록하는지 검증한다.
   - 삭제/미존재 상품은 제외하고, DB 인프라 예외는 실패로 처리하며 done key를 기록하지 않는지 검증한다.
   - carry over가 `ranking:handled:{today}`를 수정하지 않는지 검증한다.
   - carry over 실패가 `RankingKafkaConsumer`의 실시간 랭킹 적재 흐름과 결합되지 않는지 검증한다.
2. **Green**
   - `RankingKeyPolicy`에 carry over done key 계산을 추가한다.
   - `RankingRedisRepository`에 Top N 조회, carry over done key 확인/기록 기능을 추가한다.
   - `RankingCarryOverJob`을 추가해 전일 Top N 조회, 상품 DB 필터링, 점수 이월, TTL 설정, done key 기록을 수행한다.
   - 스케줄러는 00:10 이전 실행만 허용하고, 이후에는 skip한다.
3. **Refactor**
   - Top N, 감쇠율, 허용 cutoff, TTL 상수의 위치를 명확히 정리한다.
   - `RankingRebuildJob`과 `RankingCarryOverJob`의 책임이 섞이지 않도록 메서드와 패키지명을 정리한다.
   - E2E 검증은 선택 확장 항목으로 두고, 기본 완료 기준에는 단위/통합 테스트만 포함한다.

**검증:** `./gradlew :modules:ranking-contract:test :apps:commerce-streamer:test`

### Step 9. E2E 흐름 검증

**목표:** 이벤트 발행부터 API 조회까지 전체 흐름이 설계대로 이어지는지 확인한다.

1. **Red**
   - Testcontainers 기반 Redis/Kafka 통합 테스트를 작성한다.
   - 조회/좋아요/주문 이벤트를 발행한 뒤 ZSET 점수와 Ranking API 응답을 검증한다.
   - 같은 `eventId`를 두 번 처리해도 ZSET 점수가 한 번만 반영되는지 검증한다.
   - 이벤트 발생일이 전일인 이벤트가 전일 Key에 반영되고 TTL 내 조회 가능한지 검증한다.
   - `MetricsKafkaConsumer`와 `RankingKafkaConsumer`가 독립 Consumer Group으로 같은 이벤트를 각각 처리하는지 검증한다.
   - Redis 랭킹 Key 삭제 후 `OUTBOX_EVENTS`의 `PRODUCT_RANKING_EVENT` 기반 재빌드로 오늘/전일 랭킹이 복구되는지 검증한다.
2. **Green**
   - 필요한 테스트 설정과 fixture만 최소 추가한다.
   - API와 Consumer를 실제 빈으로 묶어 흐름을 검증한다.
3. **Refactor**
   - 테스트 fixture 중복을 줄인다.
   - 느린 E2E 테스트와 빠른 단위 테스트를 분리해 실행 비용을 관리한다.

**검증:** `./gradlew :apps:commerce-api:test :apps:commerce-streamer:test :tests:commerce-e2e:test`

## 4. 선택적 최적화: Kafka 배치 리스너

> 상태: 보류. Step 1~9의 기본 단건 처리 및 carry over/재빌드/E2E 검증이 완료된 뒤 별도 의사결정으로 진행한다.

기본 단건 `RankingKafkaConsumer` 구현이 검증된 뒤에만 진행한다.

1. **Red**
   - 여러 랭킹 이벤트가 한 배치로 들어왔을 때 `(dateKey, productId)` 단위로 score가 합산되는지 테스트한다.
   - 배치 처리 결과가 단건 처리 결과와 동일한지 검증한다.
   - 상품 삭제 이벤트가 배치에 포함되어도 `ZREM`이 누락되지 않는지 검증한다.
2. **Green**
   - `RankingKafkaConsumer`에 Kafka batch listener 설정을 추가한다.
   - Redis Pipeline으로 `ZINCRBY`와 `ZREM` 호출 비용을 줄인다.
3. **Refactor**
   - 단건 처리와 배치 처리의 공통 점수 계산 로직을 공유한다.
   - 부분 실패 시 Ack 정책을 문서화하고 테스트로 고정한다.

## 5. 완료 기준

- [x] `RankingScorePolicy`가 이벤트 타입별 점수와 발생일 기준 dateKey를 계산한다.
- [x] `ranking:all:{yyyyMMdd}`와 `ranking:handled:{yyyyMMdd}`가 모두 2일 TTL로 생성된다.
- [x] 같은 `eventId`가 재처리되어도 Redis ZSET 점수가 중복 가산되지 않는다.
- [x] `MetricsKafkaConsumer`와 `RankingKafkaConsumer`가 서로 다른 Consumer Group으로 같은 이벤트를 독립 소비한다.
- [x] `MetricsKafkaConsumer`는 `product_metrics`만 갱신하고, `RankingKafkaConsumer`는 Redis 랭킹만 갱신한다.
- [x] `RankingKafkaConsumer`는 Redis 반영 성공 후 Kafka offset을 커밋한다.
- [x] `modules:ranking-contract`가 랭킹 표준 이벤트, 점수 정책, Key/date 정책을 제공한다.
- [x] `commerce-api`와 `commerce-streamer`가 같은 `RankingKeyPolicy`를 사용한다.
- [x] `modules:event-contract`가 읽기 전용 `OutboxEventLog` 계약을 제공한다.
- [x] `PRODUCT_RANKING_EVENT` raw payload에는 `eventId`를 저장하지 않고, Relay/재빌드 조회 시 `OUTBOX_EVENTS.id`를 주입한다.
- [x] `GET /api/v1/rankings`가 상품 정보를 포함한 랭킹 페이지를 반환한다.
- [x] 상품 논리 삭제 시 `rankingEventType=PRODUCT_DELETED`인 `PRODUCT_RANKING_EVENT`가 발행되고, `RankingKafkaConsumer`가 최근 TTL 범위의 랭킹 ZSET에서 해당 상품을 제거한다.
- [x] 삭제 이벤트 처리 지연 등으로 ZSET에 남아 있는 삭제 상품은 API 응답에서 제외된다.
- [x] 상품 상세 조회가 오늘 기준 랭킹 정보를 포함하고, 랭킹이 없으면 `null`을 반환한다.
- [x] Redis 데이터 유실 시 `OUTBOX_EVENTS`의 `PRODUCT_RANKING_EVENT`를 조회해 오늘/전일 랭킹을 복구할 수 있다.
- [x] 재빌드 조회는 `INIT`, `COMPLETED` 상태를 포함하고 `FAILED` 상태를 제외한다.
- [x] 재빌드 후보는 `createdAt` 기준 버퍼 기간으로 조회하고, 실제 반영 날짜는 payload의 `occurredAt` 기준으로 계산한다.
- [x] 운영 Key 교체 중 실시간 Consumer 충돌 방지는 후속 운영 절차로 명시되어 있다.
- [x] 이벤트 발행 -> Consumer 처리 -> Redis ZSET 반영 -> API 조회 E2E 테스트가 통과한다.

- [ ] `RankingCarryOverJob`이 전일 Top 1,000 랭킹 점수의 10%를 오늘 랭킹 초기 점수로 적재한다.
- [ ] carry over 중복 실행이 `ranking:carry-over:done:{yyyyMMdd}` Key로 방지되고 TTL은 2일이다.
- [ ] carry over는 `ranking:handled:{yyyyMMdd}`를 수정하지 않는다.
- [ ] carry over는 00:10 이후 실행 시 skip된다.
- [ ] 전일 랭킹이 없거나 비어 있으면 점수 적재 없이 done key만 기록한다.
- [ ] carry over 시 삭제/미존재 상품은 제외하고, DB 인프라 예외는 실패로 처리한다.
- [ ] carry over 실패와 무관하게 `RankingKafkaConsumer`의 실시간 랭킹 적재는 계속된다.

## 6. 커밋 단위 제안

1. `test: 랭킹 점수 정책 테스트 추가`
2. `feat: 랭킹 계약 모듈 구현`
3. `test: Redis 랭킹 저장소 멱등성 테스트 추가`
4. `feat: Redis 랭킹 저장소 구현`
5. `test: 랭킹 Consumer 이벤트 처리 테스트 추가`
6. `feat: Ranking Consumer 랭킹 적재 구현`
7. `test: 랭킹 조회 API 테스트 추가`
8. `feat: 인기상품 랭킹 API 구현`
9. `test: 상품 삭제 시 랭킹 표준 이벤트 테스트 추가`
10. `feat: 상품 삭제 랭킹 이벤트 발행 구현`
11. `test: 상품 상세 랭킹 응답 테스트 추가`
12. `feat: 상품 상세 조회에 오늘 랭킹 포함`
13. `test: 이벤트 로그 계약 테스트 추가`
14. `feat: 이벤트 로그 계약 모듈 구현`
15. `test: Outbox 기반 랭킹 재빌드 테스트 추가`
16. `feat: Outbox 기반 랭킹 재빌드 구현`
17. `test: 랭킹 E2E 흐름 검증 추가`
18. `test: 랭킹 carry over 테스트 추가`
19. `feat: 랭킹 carry over 작업 구현`
