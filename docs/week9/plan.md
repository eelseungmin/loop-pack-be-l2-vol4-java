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
- 상품 상세 조회의 랭킹은 항상 서버의 오늘 날짜 기준이며, 랭킹에 없으면 `null`이다.
- 상품 논리 삭제 시 `PRODUCT_DELETED` Outbox 이벤트를 발행하고, `RankingKafkaConsumer`가 최근 TTL 범위의 Redis ZSET에서 해당 상품을 제거한다.
- 랭킹 API 조회 시점에 남아 있는 삭제 상품은 2차 방어로 제외한다.
- Redis 데이터 유실 시 오늘/전일 랭킹을 `ranking:rebuild:*` 임시 Key에 재계산한 뒤 운영 Key로 교체하는 재빌드 로직과 포트를 둔다.
- 단, 실제 `OUTBOX_EVENTS` 조회 구현은 이번 범위에서 보류한다.

## 3. 단계별 구현 계획

### Step 1. Ranking 점수 정책 작성

**목표:** 이벤트 타입별 점수와 이벤트 발생 시각 기준 dateKey 계산을 순수 로직으로 분리한다.

1. **Red**
   - `RankingScorePolicyTest` 작성.
   - 조회 이벤트는 `0.1`, 좋아요 이벤트는 `0.2`를 반환하는지 검증한다.
   - 주문 이벤트는 `0.6 * log(price * amount + 1)`을 반환하는지 검증한다.
   - 이벤트 발생 시각이 `2026-07-14T23:59:59`이면 dateKey가 `20260714`인지 검증한다.
2. **Green**
   - `RankingScorePolicy`를 최소 구현한다.
   - 이벤트 타입별 Weight는 상수로 둔다.
3. **Refactor**
   - 이벤트 타입 문자열, Weight, date formatter를 명확한 이름으로 정리한다.
   - 정책 클래스는 Redis, Kafka, DB에 의존하지 않게 유지한다.

**검증:** `./gradlew :apps:commerce-streamer:test`

### Step 2. Redis 랭킹 저장소 구현

**목표:** Redis ZSET과 handled Set을 다루는 저장소를 만든다.

1. **Red**
   - `RankingRedisRepositoryTest` 작성.
   - `ranking:handled:{yyyyMMdd}`에 `eventId`가 최초 저장될 때만 true를 반환하는지 검증한다.
   - 최초 처리 이벤트만 `ranking:all:{yyyyMMdd}` 점수를 증가시키는지 검증한다.
   - ZSET과 handled Set에 모두 2일 TTL이 설정되는지 검증한다.
2. **Green**
   - `RankingRedisRepository` 인터페이스와 Redis 구현체를 추가한다.
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
   - `ProductAdminFacadeTest`에 상품 삭제 시 `PRODUCT_DELETED` Outbox 이벤트가 저장되는지 테스트를 추가한다.
   - `RankingKafkaConsumerTest`에 상품 삭제 이벤트 수신 시 `ranking:all:{today}`, `ranking:all:{yesterday}`에서 해당 `productId`가 제거되는지 테스트를 추가한다.
   - 삭제된 상품이 ZSET에서 제거된 뒤 랭킹 API 다음 페이지와 중복 노출되지 않는지 검증한다.
2. **Green**
   - `ProductAdminFacade.deleteProduct` 흐름에서 상품 논리 삭제와 함께 `PRODUCT_DELETED` Outbox 이벤트를 저장한다.
   - `RankingRedisRepository.removeProductFromRecentRankings(productId)`를 구현한다.
   - `RankingKafkaConsumer`가 상품 삭제 이벤트를 소비해 최근 2일 랭킹 Key에 대해 `ZREM`을 수행한다.
3. **Refactor**
   - TTL 기간이 바뀌어도 제거 대상 Key 계산을 한 곳에서 관리하도록 정리한다.
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

### Step 7. Redis 랭킹 유실 재빌드 Job

**목표:** Redis 랭킹 데이터가 유실된 경우 원천 이벤트 로그를 이용해 오늘/전일 랭킹을 재계산할 수 있도록 재빌드 Job과 이벤트 조회 포트를 마련한다.

> 범위 조정: 실제 `OUTBOX_EVENTS` 조회 구현은 보류한다. 이번 단계에서는 `RankingRebuildEventRepository` 포트, 재계산 로직, Redis 임시 Key 교체 로직까지만 구현한다.

1. **Red**
   - `RankingRebuildJobTest`를 작성한다.
   - 오늘/전일 범위의 조회/좋아요/주문 이벤트를 읽어 `ranking:rebuild:all:{yyyyMMdd}`에 점수가 재계산되는지 검증한다.
   - 상품 삭제 이벤트가 재빌드 결과에서 해당 상품을 제거하는지 검증한다.
   - 재빌드 완료 후 `ranking:rebuild:*` 임시 Key가 운영 Key(`ranking:*`)로 교체되는지 검증한다.
2. **Green**
   - `RankingRebuildJob`을 구현한다.
   - 대상 기간의 랭킹 관련 이벤트는 `RankingRebuildEventRepository` 포트로 조회한다.
   - `RankingScorePolicy`를 재사용해 점수를 계산하고 Redis 임시 Key에 적재한다.
   - 재빌드 완료 후 임시 Key를 운영 Key로 교체한다.
3. **Refactor**
   - 재빌드 대상 기간 계산(today/yesterday)을 설정 값 또는 정책 클래스로 분리한다.
   - 실제 `OUTBOX_EVENTS` 조회 구현과 운영 Key 교체 중 Consumer 충돌 방지 절차는 후속 의사결정으로 남긴다.

**검증:** `./gradlew :apps:commerce-streamer:test`

### Step 8. E2E 흐름 검증

**목표:** 이벤트 발행부터 API 조회까지 전체 흐름이 설계대로 이어지는지 확인한다.

1. **Red**
   - Testcontainers 기반 Redis/Kafka 통합 테스트를 작성한다.
   - 조회/좋아요/주문 이벤트를 발행한 뒤 ZSET 점수와 Ranking API 응답을 검증한다.
   - 같은 `eventId`를 두 번 처리해도 ZSET 점수가 한 번만 반영되는지 검증한다.
   - 이벤트 발생일이 전일인 이벤트가 전일 Key에 반영되고 TTL 내 조회 가능한지 검증한다.
   - `MetricsKafkaConsumer`와 `RankingKafkaConsumer`가 독립 Consumer Group으로 같은 이벤트를 각각 처리하는지 검증한다.
   - Redis 랭킹 Key 삭제 후 재빌드 Job의 포트 기반 재계산 로직으로 오늘/전일 랭킹이 복구되는지 검증한다.
2. **Green**
   - 필요한 테스트 설정과 fixture만 최소 추가한다.
   - API와 Consumer를 실제 빈으로 묶어 흐름을 검증한다.
3. **Refactor**
   - 테스트 fixture 중복을 줄인다.
   - 느린 E2E 테스트와 빠른 단위 테스트를 분리해 실행 비용을 관리한다.

**검증:** `./gradlew :apps:commerce-api:test :apps:commerce-streamer:test`

## 4. 선택적 최적화: Kafka 배치 리스너

> 상태: 보류. Step 1~8의 기본 단건 처리 및 재빌드/E2E 검증이 완료된 뒤 별도 의사결정으로 진행한다.

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

- [ ] `RankingScorePolicy`가 이벤트 타입별 점수와 발생일 기준 dateKey를 계산한다.
- [ ] `ranking:all:{yyyyMMdd}`와 `ranking:handled:{yyyyMMdd}`가 모두 2일 TTL로 생성된다.
- [ ] 같은 `eventId`가 재처리되어도 Redis ZSET 점수가 중복 가산되지 않는다.
- [ ] `MetricsKafkaConsumer`와 `RankingKafkaConsumer`가 서로 다른 Consumer Group으로 같은 이벤트를 독립 소비한다.
- [ ] `MetricsKafkaConsumer`는 `product_metrics`만 갱신하고, `RankingKafkaConsumer`는 Redis 랭킹만 갱신한다.
- [ ] `RankingKafkaConsumer`는 Redis 반영 성공 후 Kafka offset을 커밋한다.
- [ ] `GET /api/v1/rankings`가 상품 정보를 포함한 랭킹 페이지를 반환한다.
- [ ] 상품 논리 삭제 시 `PRODUCT_DELETED` Outbox 이벤트가 발행되고, `RankingKafkaConsumer`가 최근 TTL 범위의 랭킹 ZSET에서 해당 상품을 제거한다.
- [ ] 삭제 이벤트 처리 지연 등으로 ZSET에 남아 있는 삭제 상품은 API 응답에서 제외된다.
- [ ] 상품 상세 조회가 오늘 기준 랭킹 정보를 포함하고, 랭킹이 없으면 `null`을 반환한다.
- [ ] Redis 데이터 유실 시 포트 기반 재빌드 로직으로 오늘/전일 랭킹을 복구할 수 있다.
- [ ] 실제 `OUTBOX_EVENTS` 조회 구현은 보류 상태로 명시되어 있다.
- [ ] 이벤트 발행 -> Consumer 처리 -> Redis ZSET 반영 -> API 조회 E2E 테스트가 통과한다.

## 6. 커밋 단위 제안

1. `test: 랭킹 점수 정책 테스트 추가`
2. `feat: 랭킹 점수 정책 구현`
3. `test: Redis 랭킹 저장소 멱등성 테스트 추가`
4. `feat: Redis 랭킹 저장소 구현`
5. `test: 랭킹 Consumer 이벤트 처리 테스트 추가`
6. `feat: Ranking Consumer 랭킹 적재 구현`
7. `test: 랭킹 조회 API 테스트 추가`
8. `feat: 인기상품 랭킹 API 구현`
9. `test: 상품 삭제 시 랭킹 ZSET 제거 테스트 추가`
10. `feat: 상품 삭제 이벤트 기반 랭킹 ZSET 제거`
11. `test: 상품 상세 랭킹 응답 테스트 추가`
12. `feat: 상품 상세 조회에 오늘 랭킹 포함`
13. `test: Redis 랭킹 재빌드 테스트 추가`
14. `feat: Redis 랭킹 재빌드 Job 구현`
15. `test: 랭킹 E2E 흐름 검증 추가`
