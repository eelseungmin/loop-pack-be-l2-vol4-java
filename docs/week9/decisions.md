문제 재정리
이 요구사항의 핵심은 “상품 이벤트를 이미 product_metrics에 누적 집계하고 있는데, 이 데이터를 응용해서 실시간 인기상품 랭킹 API까지 제공하고 싶다”입니다.
다만 여기에는 두 가지 책임이 섞여 있습니다.
product_metrics
누적 좋아요/조회/판매 같은 영속 집계 데이터. 정산, 통계, 장기 조회에 가까움.

Redis ZSET 랭킹
ranking:all:{yyyyMMdd} 키로 관리되는 일간 실시간 조회 모델. 빠른 랭킹 API 응답에 가까움.

그래서 설계 문서에는 “product_metrics를 대체한다”가 아니라, “Kafka Consumer가 이벤트를 소비하면서 product_metrics 누적 집계와 Redis ZSET 일간 랭킹 적재를 함께 수행한다”는 방향이 자연스럽습니다.
불확실한 지점
여기서 바로 문서를 고치면 안 되고, 먼저 결정해야 할 질문들이 있습니다.

첫 번째 질문입니다.
랭킹을 날짜별 ZSET(ranking:all:{yyyyMMdd})에 넣을 때, 날짜 기준은 무엇으로 잡을까요?
선택지 A: 이벤트 발생 시각 기준
예: 상품 조회/좋아요/주문이 7월 14일 23:59에 발생했으면, Consumer가 7월 15일에 처리해도 ranking:all:20260714에 반영
장점: “그날 실제로 일어난 인기”에 가장 정확함
단점: 이벤트 payload에 발생 시각이 반드시 있어야 하고, 지연 이벤트가 과거 랭킹을 수정할 수 있음

선택지 B: Consumer 처리 시각 기준
예: Consumer가 7월 15일 00:01에 처리했으면 ranking:all:20260715에 반영
장점: 구현이 단순하고 payload 의존이 적음
단점: 자정 전후 지연 처리로 실제 발생일과 랭킹 날짜가 어긋날 수 있음

두 번째 질문입니다.
주문 이벤트의 랭킹 점수를 어떻게 계산할까요?
선택지 A: price * amount 그대로 사용
예: 주문 점수 = 0.6 * price * amount
장점: 매출 기여도가 랭킹에 강하게 반영됨
단점: 고가 상품 1건이 저가 상품의 여러 주문/좋아요/조회보다 과도하게 커질 수 있음

선택지 B: 로그 정규화 사용
예: 주문 점수 = 0.6 * log(price * amount + 1)
장점: 고가 상품 쏠림을 줄이고 조회/좋아요/주문 간 균형이 좋아짐
단점: 점수 설명이 조금 어려워지고, 요구사항 예시보다 계산식이 복잡해짐

선택지 C: 주문도 단순 건수 기반
예: 주문 점수 = 0.6 * 1
장점: 가장 단순하고 “주문 1건 > 좋아요 3건” 같은 검증이 명확함
단점: 주문 금액 차이를 전혀 반영하지 못함

세 번째 질문입니다.
Redis ZSET 랭킹 반영을 어느 Consumer에서 처리할까요?
선택지 A: 기존 MetricsKafkaConsumer에서 함께 처리
흐름: 이벤트 수신 → product_metrics 갱신 → Redis ZSET 랭킹 반영 → Ack
장점: Consumer가 하나라 흐름이 단순하고, 기존 product_metrics 집계 파이프라인을 그대로 응용하기 좋음
단점: product_metrics와 랭킹 적재 책임이 한 Consumer에 같이 들어감

선택지 B: 별도 RankingKafkaConsumer로 분리
흐름: 같은 Kafka 이벤트를 MetricsKafkaConsumer와 RankingKafkaConsumer가 각각 소비
장점: 누적 통계와 실시간 랭킹 책임이 분리됨
단점: Consumer group, 멱등성, 장애 처리 지점이 늘어나고 설계가 복잡해짐

선택지 C: product_metrics 갱신 후 별도 스케줄러가 Redis 랭킹 생성
장점: Kafka Consumer는 DB 집계만 담당해서 단순함
단점: “실시간 랭킹” 요구와는 거리가 생기고, Redis ZSET을 이벤트 기반으로 누적한다는 요구사항과도 덜 맞음

네 번째 질문입니다. 이게 꽤 중요합니다.
Kafka는 재처리될 수 있고, Redis ZINCRBY는 같은 이벤트를 두 번 실행하면 점수가 두 번 올라갑니다.
그러면 Redis 랭킹 중복 가산 방지를 어떻게 설계할까요?
선택지 A: DB의 event_handled 저장 이후 Redis 반영
흐름: event_handled 확인 → product_metrics 갱신 → event_handled 저장 → Redis ZSET 반영 → Ack
장점: 기존 DB 멱등성 테이블을 재사용
단점: Redis 반영 후 Ack 전에 장애가 나면, 재처리 시 DB는 “이미 처리됨”으로 보고 Redis 반영을 스킵할지/재실행할지 애매해짐

선택지 B: Redis에도 별도 처리 이력 Set을 둠
예: ranking:handled:{yyyyMMdd} Set에 eventId 저장
흐름: Redis에서 SADD ranking:handled:{date} eventId 성공 시에만 ZINCRBY
장점: Redis ZSET 중복 가산을 직접 방어할 수 있음
단점: Redis 키가 하나 더 생기고, SADD + ZINCRBY + EXPIRE를 원자적으로 묶는 Lua Script 또는 트랜잭션 설계가 필요함

선택지 C: 중복 가산을 허용하고 주기적 보정 배치로 복구
장점: 실시간 처리 구현은 단순함
단점: 랭킹이 일시적으로 틀릴 수 있고, “실시간 인기상품” 신뢰도가 떨어짐

다섯 번째 질문입니다.
상품 상세 조회에서 “해당 상품의 랭킹 정보”를 추가할 때, 어느 날짜 기준의 랭킹을 보여줄까요?
선택지 A: 항상 오늘 기준
예: GET /api/v1/products/{productId} 호출 시 서버 오늘 날짜의 ranking:all:{today}에서 순위 조회
장점: API가 단순하고 “오늘의 인기상품” 요구와 잘 맞음
단점: 과거 날짜 기준 상세 랭킹은 조회할 수 없음

선택지 B: 상품 상세 API에도 date 파라미터 추가
예: GET /api/v1/products/{productId}?rankingDate=yyyyMMdd
장점: 과거 랭킹 기준 상품 상세 조회가 가능함
단점: 상품 상세 API 책임이 살짝 넓어지고, 요구사항에 없는 파라미터가 추가됨

선택지 C: 랭킹 API에서만 랭킹 제공하고, 상품 상세에는 순위 미포함
장점: API 책임 분리가 가장 명확함
단점: 체크리스트의 “상품 상세 조회 시 해당 상품의 순위가 함께 반환된다”를 만족하지 못함

다음 질문으로 넘어가겠습니다.
랭킹 API에서 Redis ZSET에 있는 상품 ID를 조회한 뒤 상품 정보를 조합할 때, 논리 삭제된 상품은 어떻게 처리할까요?
선택지 A: 응답에서 제외
장점: 사용자가 삭제된 상품을 보지 않음
단점: Redis 랭킹 순위와 실제 응답 개수가 달라질 수 있음. 예를 들어 size=20 요청했는데 삭제 상품이 끼어 있으면 18개만 내려갈 수 있음

선택지 B: 삭제 상품도 랭킹에는 노출하되 isDeleted 표시
장점: Redis 랭킹 순위와 응답 개수가 보존됨
단점: 고객용 인기상품 API에 삭제 상품이 노출되어 부자연스러움

선택지 C: 삭제 상품을 제외하되 부족한 개수만큼 ZSET에서 더 읽어 보충
장점: 고객에게 삭제 상품을 숨기면서도 size에 가깝게 채울 수 있음
단점: 구현이 A보다 조금 복잡함. ZSET을 넉넉히 조회하거나 반복 조회해야 함

Redis ZSET과 ranking:handled:{yyyyMMdd} Set의 TTL은 둘 다 2일로 맞출까요?
선택지 A: 둘 다 2일
장점: 키 생명주기가 단순하고, “전일 랭킹 조회 가능”과 “전일 이벤트 재처리 중복 방지” 범위가 일치함
단점: 2일보다 늦게 재처리되는 이벤트는 Redis 멱등성 Set이 사라져 중복 가산 가능성이 있음

선택지 B: ZSET은 2일, handled Set은 더 길게
예: ZSET 2일, handled Set 3~7일
장점: 지연 재처리/재시작에 더 안전함
단점: Redis 메모리 사용량이 늘고, 요구사항의 TTL 2Day보다 관리 정책이 복잡해짐

선택지 C: Redis handled Set 없이 DB event_handled만 장기 멱등성으로 사용
이미 B 대신 4번에서 Redis Set을 쓰기로 했으니, 지금 기준에서는 후퇴에 가깝습니다.

아홉 번째 질문입니다.
Nice to Have인 Kafka 배치 리스너는 설계 문서에서 어느 수준으로 다룰까요?
선택지 A: 필수 설계에 포함
문서상 기본 Consumer가 배치 리스너라고 명시
장점: 요구사항의 성능 의도를 적극 반영
단점: 구현 복잡도가 올라가고, 단건 처리보다 테스트/멱등성/부분 실패 설명이 더 필요함

선택지 B: 선택적 최적화로 분리
기본 설계는 단건 Consumer 기준으로 명확히 하고, 배치 리스너는 “트래픽 증가 시 적용할 최적화”로 문서화
장점: 기본 흐름이 단순하고 검증하기 쉬움
단점: Nice to Have를 적극 구현한 설계처럼 보이진 않을 수 있음

선택지 C: 설계 문서에서는 언급만 하고 구체화하지 않음
장점: 문서가 짧아짐
단점: 체크리스트의 “너무 많은 ZSET/DB 연산” 문제를 충분히 다루지 못함

RankingKafkaConsumer가 처리할 이벤트 타입 범위를 어디까지 둘까요?
선택지 A: 조회/좋아요/주문 이벤트만 처리
랭킹 점수 계산에 직접 필요한 이벤트만 소비
장점: Consumer 책임이 명확하고 단순함
단점: 상품 삭제 이벤트는 별도 흐름에서 처리해야 함

선택지 B: 조회/좋아요/주문 + 상품 삭제 이벤트까지 처리
랭킹 점수 적재와 삭제 상품 ZSET 제거를 같은 Consumer가 처리
장점: 랭킹 read model 변경 책임이 RankingKafkaConsumer에 모임
단점: 상품 삭제 이벤트도 Kafka로 발행되어야 하고, 랭킹 Consumer가 처리하는 이벤트 타입이 늘어남

선택지 C: 모든 상품 관련 이벤트를 일단 소비하고 내부에서 분기
장점: 확장 여지는 큼
단점: 지금 요구보다 과하고, 불필요한 이벤트까지 랭킹 Consumer가 알게 됨

현재 문서에는 상품 삭제 시 ProductAdminFacade가 직접 Redis ZSET에서 제거하는 흐름으로 되어 있습니다.
B 구조로 Consumer를 분리하더라도, 삭제 처리는 그대로 두려면 A가 가장 작고 안전합니다.
반대로 랭킹 read model 변경을 모두 이벤트 기반으로 일관되게 가져가려면 B가 더 설계적으로 깔끔합니다.
저는 지금 변경 범위를 줄인다면 A, 랭킹 read model 책임을 더 명확히 하려면 B라고 봅니다.
어느 쪽으로 갈까요?

상품 삭제 이벤트는 어떤 방식으로 Kafka까지 전달할까요?
선택지 A: 기존 Outbox 파이프라인 사용
상품 삭제 트랜잭션 안에서 OUTBOX_EVENTS에 rankingEventType=PRODUCT_DELETED인 PRODUCT_RANKING_EVENT 저장
Outbox Relay가 Kafka로 발행
RankingKafkaConsumer가 소비해 Redis ZSET에서 제거
장점: 상품 삭제 DB 변경과 이벤트 발행 요청이 같은 트랜잭션 안에 묶여 유실 위험이 낮음
단점: Outbox 이벤트 타입과 payload가 추가됨

선택지 B: ProductAdminFacade에서 Kafka 직접 발행
상품 삭제 후 Kafka Producer 직접 호출
장점: 구현은 단순해 보임
단점: DB 삭제 성공 후 Kafka 발행 실패 시 랭킹 ZSET 정리가 유실될 수 있음

선택지 C: 상품 삭제 후 동기 Redis ZREM도 하고 Kafka 이벤트도 발행
장점: 즉시 제거 + 이벤트 기반 보정 가능
단점: 책임이 중복되고, 장애/재처리 정책이 복잡해짐

현재 프로젝트가 이미 Outbox를 중요한 이벤트 전달 보장 방식으로 쓰고 있으므로, 저는 A: 기존 Outbox 파이프라인 사용이 맞다고 봅니다.

네 번째 질문입니다.
RankingKafkaConsumer의 Redis 멱등성 Key는 상품 삭제 이벤트에도 동일하게 ranking:handled:{yyyyMMdd}를 사용할까요?
여기서 문제가 하나 있습니다. 조회/좋아요/주문 이벤트는 특정 날짜의 점수에 반영되므로 ranking:handled:{eventDate}가 자연스럽습니다.
그런데 상품 삭제 이벤트는 “삭제 발생일”의 랭킹만 지우는 게 아니라, TTL 범위의 여러 랭킹 Key, 즉 오늘/전일에서 해당 상품을 제거해야 합니다.
선택지는 이렇습니다.
선택지 A: 삭제 이벤트도 ranking:handled:{deletedAt yyyyMMdd}에 기록
장점: 기존 handled Key 전략을 그대로 씀
단점: 삭제 이벤트는 여러 ZSET Key를 수정하므로, 멱등성 Key와 실제 수정 범위가 1:1로 맞지 않음

선택지 B: 삭제 이벤트 전용 handled Key 사용
예: ranking:handled:product-delete:{yyyyMMdd}
장점: 점수 누적 이벤트와 삭제 이벤트의 성격을 분리할 수 있음
단점: Key가 하나 더 늘어남

선택지 C: 삭제 이벤트는 멱등성 Set 없이 ZREM만 수행
Redis ZREM은 같은 상품을 여러 번 제거해도 결과가 같으므로 멱등적입니다.
장점: 가장 단순함
단점: 처리 이력을 Redis에서 추적하지 않음. 다만 Kafka consumer의 Ack/retry 관점에서는 큰 문제는 적음

다섯 번째 질문입니다.
RankingKafkaConsumer와 MetricsKafkaConsumer가 독립 Consumer Group으로 동작하면, 한쪽은 성공하고 다른 한쪽은 실패할 수 있습니다.
이때 두 read model의 일시적 불일치를 어떻게 볼까요?
예를 들어:
MetricsKafkaConsumer 성공 -> product_metrics 반영 완료
RankingKafkaConsumer 실패 -> Redis 랭킹 미반영
또는 반대로:
MetricsKafkaConsumer 실패 -> product_metrics 미반영
RankingKafkaConsumer 성공 -> Redis 랭킹 반영 완료
선택지는 이렇습니다.
선택지 A: 일시적 불일치를 허용한다
두 read model은 서로 다른 목적의 projection으로 본다.
각 Consumer가 독립적으로 재시도해 eventual consistency를 맞춘다.
장점: Consumer 분리의 장점을 살릴 수 있음
단점: 짧은 시간 동안 상품 목록의 product_metrics와 랭킹 API 결과가 다를 수 있음

선택지 B: 둘 중 하나가 실패하면 다른 쪽도 롤백되도록 강하게 묶는다
사실 Kafka Consumer Group이 분리된 상태에서는 구현이 어렵고, 분리한 의미가 약해집니다.
장점: 일관성은 높아짐
단점: 구조가 복잡하고 장애 전파가 생김

선택지 C: Ranking은 항상 product_metrics를 기준으로 재계산한다
이건 사실 선택지 C였던 스케줄러/재집계 방식에 가까워집니다.
실시간 ZSET 누적 구조와는 달라집니다.

RankingKafkaConsumer가 Redis 랭킹 적재에 실패했을 때, Kafka Ack/커밋 정책은 어떻게 가져갈까요?
선택지 A: Redis 반영 성공 후 Ack
Redis SADD/ZINCRBY/EXPIRE 또는 삭제 이벤트의 ZREM이 성공한 뒤에만 offset commit
실패하면 Ack하지 않고 재처리
장점: Redis 랭킹 반영 누락을 최소화
단점: Redis 장애 중에는 Ranking Consumer lag이 쌓임

일곱 번째 질문입니다.
이제 Redis 장애 복구/재빌드 정책을 문서에 어느 수준으로 넣을까요?
B 구조에서는 RankingKafkaConsumer가 Redis 반영 성공 후 Ack하지 않기 때문에, Redis 장애가 짧으면 Consumer lag이 쌓였다가 복구 후 밀린 이벤트를 처리하면 됩니다.
하지만 Redis 데이터가 유실되거나 flush된 경우에는 Kafka lag만으로는 이미 Ack된 과거 이벤트를 복원할 수 없습니다.
선택지는 이렇습니다.
선택지 A: 이번 설계에서는 Redis 장애 중 lag 누적/재처리까지만 다룸
Redis가 잠깐 장애였다가 데이터는 유지되는 상황만 고려
장점: 구현 범위가 작음
단점: Redis 데이터 유실/flush 시 랭킹 재빌드 전략이 없음

선택지 B: Redis 데이터 유실 시 Outbox/Event 로그 기반 재빌드 정책까지 문서화
오늘/전일 이벤트를 OUTBOX_EVENTS에서 다시 읽어 ranking:rebuild:* 임시 Key에 재계산
완료 후 운영 Key로 교체
장점: Redis를 파생 read model로 보는 설계가 완성됨
단점: 구현 계획이 늘어나고, 재빌드 job/운영 절차가 필요함

선택지 C: Redis persistence/RDB/AOF에 의존하고 애플리케이션 재빌드는 다루지 않음
장점: 애플리케이션 설계는 단순함
단점: Redis 운영 설정에 강하게 의존하고, 애플리케이션 문서상 복구 설명이 약함

문제 재정리:
사용자 관점: Redis 랭킹이 유실되어도 오늘/전일 인기상품 랭킹을 복구할 수 있어야 함
비즈니스 관점: Redis 랭킹은 파생 read model이므로, 원천 이벤트 로그만 살아 있으면 재생성 가능해야 함
시스템 관점: 현재 outbox_events 모델은 commerce-api 쪽에 있고, RankingRebuildJob은 commerce-streamer에 있으므로 직접 구현하려면 공용 계약/조회 모델이 필요함
첫 번째 질문입니다.
공용 모듈을 어디까지 분리할까요?
선택지 A: modules/event-contract만 만든다
OUTBOX_EVENTS 조회에 필요한 공통 이벤트 로그 모델만 분리합니다.
예: OutboxEventLog, OutboxEventType, OutboxPayload
장점: 이번 C→B 변경의 직접 문제에 집중함
단점: 랭킹 key/score/event contract 중복 문제는 별도 후속 과제로 남음
선택지 B: modules/ranking-contract만 만든다
랭킹 재빌드에 필요한 계약을 랭킹 중심으로 분리합니다.
예: RankingEventType, RankingRebuildEvent, RankingScorePolicy, ranking key 정책
장점: API/Streamer 양쪽에 흩어진 랭킹 비즈니스 계약을 정리하기 좋음
단점: OUTBOX_EVENTS 자체 조회 모델은 여전히 어디에 둘지 애매할 수 있음
선택지 C: event-contract와 ranking-contract를 분리한다
이벤트 로그 계약과 랭킹 계약을 각각 다른 공용 모듈로 둡니다.
장점: 관심사 분리가 가장 명확함. modules/redis에 비즈니스 계약이 새지 않음
단점: 이번 변경 범위가 가장 커지고, 리팩토링 커밋이 늘어남
제 판단으로는 C가 설계적으로 가장 깔끔하지만, 현재 작업 범위를 줄이려면 A가 현실적입니다.

선택지 B: 공용 모듈에는 읽기 전용 계약만 둔다
modules/event-contract에는 OutboxEventLog 같은 read model DTO와 OutboxEventType 정도만 둡니다.
JPA Entity는 각 앱/인프라에서 별도로 둡니다.
예:
modules/event-contract
OutboxEventLog
OutboxEventType
OutboxEventStatus? 또는 조회 조건 enum

commerce-api
OutboxEvent Entity
OutboxEventRepository

commerce-streamer
OutboxEventLogJpaEntity 또는 QueryDSL/Jdbc 조회 구현
RankingRebuildEventRepositoryImpl
장점:
공용 모듈이 순수 계약에 가까움
commerce-streamer는 재빌드에 필요한 필드만 읽을 수 있음
API의 Outbox 쓰기 모델 변경이 Streamer에 덜 전파됨
단점:
같은 outbox_events 테이블에 대한 매핑/조회 코드가 일부 중복될 수 있음
EventType 이름/값 동기화가 중요함
선택지 C: 공용 모듈에는 이벤트 타입/페이로드 계약만 둔다
OutboxEventLog도 두지 않고, ProductEventPayload, RankingEventType 같은 payload contract만 공유합니다.
OUTBOX_EVENTS 조회 모델은 streamer 내부 구현으로 둡니다.
장점:
공용 모듈이 가장 가볍고 순수함
“비즈니스 이벤트 계약”만 공유한다는 원칙이 명확함
단점:
C→B 변경의 핵심인 “Outbox/Event 조회 모델 공유” 효과는 약함
재빌드 조회 구현은 여전히 streamer 내부에 많이 남음
제 추천은 B입니다.
계약 모듈은 계약답게 두고, JPA Entity까지 공유하는 건 피하는 쪽이 관심사 분리에 더 맞습니다.
어느 쪽으로 갈까요?

다음 의사결정 질문입니다.
RankingRebuildJob이 재빌드 대상으로 삼을 OUTBOX_EVENTS 범위는 어떤 상태의 이벤트까지 포함할까요?
현재 Outbox는 대략 이런 상태를 가집니다.
INIT       아직 Kafka 발행/처리 완료 전
COMPLETED  발행 또는 처리 완료
FAILED     발행/처리 실패
Redis 유실 복구 관점에서는 “이미 비즈니스 트랜잭션 안에서 기록된 원천 이벤트”를 다시 읽는 것이 목적입니다. 그런데 status를 어떻게 해석할지가 중요합니다.
선택지 A: COMPLETED만 읽는다
Kafka로 정상 발행 완료된 이벤트만 재빌드 원천으로 사용합니다.
장점:
실제로 Consumer가 처리했을 가능성이 높은 이벤트만 재생성
실패/미발행 이벤트가 랭킹에 섞이지 않음
단점:
Redis 장애 시점에 INIT 상태로 아직 발행 전이었던 이벤트는 재빌드에서 빠질 수 있음
Outbox status가 “Kafka 발행 완료”인지 “후속 처리 완료”인지 의미가 흔들리면 위험함
선택지 B: INIT, COMPLETED를 읽고 FAILED는 제외한다
비즈니스 트랜잭션에서 기록된 이벤트 중 실패 확정이 아닌 이벤트를 재빌드 원천으로 봅니다.
장점:
발행 대기 중인 이벤트도 복구 대상에 포함
Redis를 “원천 이벤트 로그로부터 재생성되는 projection”으로 보는 관점에 잘 맞음
단점:
아직 Kafka로 발행되지 않은 이벤트까지 랭킹에 먼저 반영될 수 있음
Outbox 발행과 랭킹 재빌드 결과가 일시적으로 앞서갈 수 있음
선택지 C: 상태와 무관하게 모두 읽는다
FAILED까지 포함해서 해당 기간의 모든 랭킹 관련 이벤트를 읽습니다.
장점:
누락 가능성이 가장 낮음
실패 상태가 일시적인 전송 실패라면 복구에 유리함
단점:
영구 실패 처리된 이벤트까지 랭킹에 반영될 수 있음
실패 상태의 의미를 무시하게 됨
제 추천은 B입니다.
Outbox는 비즈니스 트랜잭션 안에서 기록된 원천 로그에 가깝고, FAILED는 별도 보정/운영 판단 대상이므로 기본 재빌드에서는 제외하는 게 균형이 좋아 보입니다.
어느 쪽으로 갈까요?

좋습니다. 3차 결정은 B로 정리하겠습니다.
재빌드 원천 이벤트는 OUTBOX_EVENTS 중 INIT, COMPLETED 상태를 포함한다.
FAILED 상태는 기본 재빌드 대상에서 제외한다.
이유: 비즈니스 트랜잭션에 기록됐지만 아직 발행 대기 중인 이벤트는 projection 재생성 대상에 포함하되, 실패 확정 이벤트는 운영/보정 판단 없이 자동 반영하지 않는다.
다음 의사결정 질문입니다.
OUTBOX_EVENTS.payload를 RankingRebuildEvent로 변환할 때, 어떤 이벤트 타입을 랭킹 재빌드 대상으로 볼까요?
현재 랭킹 Consumer 관점의 이벤트는 다음입니다.
VIEW
LIKE
ORDER
PRODUCT_DELETED
그런데 실제 Outbox EventType에는 기존 시스템 이벤트도 섞여 있을 수 있습니다.
LIKE_CREATED
LIKE_DELETED
PAYMENT_COMPLETED
PAYMENT_FAILED
USER_ACTION_LOG
PRODUCT_DELETED
...
즉 “Outbox event type”과 “Ranking event type”이 1:1이 아닐 수 있습니다.
선택지 A: 랭킹 전용 이벤트 타입만 재빌드 대상으로 삼는다
Outbox에 PRODUCT_VIEWED, PRODUCT_LIKED, PRODUCT_ORDERED, PRODUCT_DELETED 같은 랭킹용 이벤트가 저장된다는 전제로 설계합니다.
장점:
payload → RankingRebuildEvent 변환이 단순함
실시간 RankingKafkaConsumer와 재빌드 Job의 입력 계약이 같아짐
랭킹 요구사항에 가장 직접적임
단점:
기존 LIKE_CREATED, PAYMENT_COMPLETED 등과 별도로 랭킹용 이벤트를 발행해야 할 수 있음
Outbox event type 추가가 필요함
선택지 B: 기존 도메인 이벤트를 랭킹 이벤트로 매핑한다
예를 들어:
LIKE_CREATED → LIKE
PAYMENT_COMPLETED 또는 주문 완료 이벤트 → ORDER
PRODUCT_DELETED → PRODUCT_DELETED
조회 이벤트는 별도 PRODUCT_VIEWED가 필요
장점:
기존 Outbox 이벤트를 최대한 재사용
이벤트 타입 추가를 줄일 수 있음
단점:
결제/주문/좋아요 도메인 이벤트 payload에서 랭킹에 필요한 productId, price, amount, occurredAt을 안정적으로 뽑아야 함
도메인 이벤트 의미가 바뀌면 랭킹 재빌드도 영향받음
조회 이벤트는 어차피 새 이벤트가 필요할 가능성이 큼
선택지 C: 랭킹 재빌드 대상은 “랭킹 표준 이벤트 계약”으로만 제한한다
modules/ranking-contract에 ProductRankingEvent 같은 표준 계약을 두고, Outbox에는 이 계약을 payload로 저장한 이벤트만 재빌드 대상으로 삼습니다.
기존 도메인 이벤트를 재사용하고 싶다면, 발행 시점에 표준 랭킹 이벤트도 별도로 기록합니다.
장점:
실시간 Consumer와 재빌드 Job이 같은 랭킹 입력 계약을 공유
기존 도메인 이벤트 payload 변화에 덜 흔들림
ranking-contract를 분리하는 이유와 가장 잘 맞음
단점:
랭킹용 표준 이벤트를 생성/기록하는 추가 설계가 필요
기존 이벤트와 중복 발행처럼 보일 수 있음
제 추천은 C입니다.
우리가 앞에서 event-contract와 ranking-contract를 분리하기로 했기 때문에, 재빌드는 “모든 Outbox를 영리하게 해석하는 기능”이 아니라 “Outbox에 남은 랭킹 표준 이벤트를 재생하는 기능”으로 두는 게 제일 깔끔합니다.
어느 쪽으로 갈까요?

다음 의사결정 질문입니다.
랭킹 표준 이벤트를 Outbox에 기록할 때, event_type은 어떻게 둘까요?
선택지 A: 이벤트 행위별로 분리
예:
PRODUCT_VIEWED
PRODUCT_LIKED
PRODUCT_ORDERED
PRODUCT_DELETED
장점:
DB에서 event_type만 보고 재빌드 대상 필터링이 쉬움
운영자가 Outbox를 볼 때 이벤트 의미가 명확함
조회/좋아요/주문/삭제별 통계나 재처리도 쉬움
단점:
Outbox EventType enum 값이 늘어남
랭킹 표준 이벤트 타입과 Outbox 이벤트 타입이 거의 중복됨
선택지 B: 하나의 랭킹 이벤트 타입으로 통합
예:
PRODUCT_RANKING_EVENT
payload 내부에 rankingEventType: VIEW | LIKE | ORDER | PRODUCT_DELETED를 둡니다.
장점:
Outbox event_type이 단순함
재빌드 대상 필터가 PRODUCT_RANKING_EVENT 하나로 끝남
랭킹 표준 이벤트 계약을 payload에 집중시킬 수 있음
단점:
DB에서 행위별 필터링은 payload 파싱이 필요함
운영자가 event_type만 봐서는 조회/좋아요/주문/삭제를 구분하기 어려움
선택지 C: 도메인 이벤트 타입 + 랭킹 이벤트 타입 둘 다 기록
예:
event_type = PRODUCT_RANKING_EVENT
payload.rankingEventType = ORDER
payload.sourceEventType = PAYMENT_COMPLETED
장점:
랭킹 표준 이벤트로 재빌드는 단순하게 하면서도 원천 도메인 이벤트 추적성이 좋아짐
나중에 “이 랭킹 이벤트가 어떤 도메인 이벤트에서 파생됐는가”를 확인 가능
단점:
payload가 조금 커지고, source 정보 정책이 추가됨
모든 이벤트에 sourceEventType을 넣을지 기준이 필요함

다음 의사결정 질문입니다.
ProductRankingEvent.eventId는 무엇을 기준으로 할까요?
이게 중요합니다. 현재 Redis 멱등성은 ranking:handled:{yyyyMMdd}에 eventId를 저장해서 중복 가산을 막습니다. 그런데 Outbox에서 재빌드까지 하려면 “Kafka 메시지에서 쓰는 eventId”와 “Outbox 원천 로그의 id” 관계가 명확해야 합니다.
선택지 A: OUTBOX_EVENTS.id를 eventId로 사용
Outbox row id를 문자열로 변환해서 ProductRankingEvent.eventId에 넣습니다.
장점:
이미 DB에서 유일함
재빌드 시 원천 로그 id와 Kafka eventId가 일치
별도 UUID 생성이 필요 없음
기존 문서의 “OUTBOX_EVENTS id를 그대로 재사용” 방향과 맞음
단점:
Outbox row가 만들어진 뒤에야 eventId를 알 수 있음
payload를 저장할 때 payload 내부 eventId를 먼저 채우기 어렵거나, 저장 후 payload 업데이트가 필요할 수 있음
또는 payload에는 eventId를 두지 않고, relay/조회 시 row id를 eventId로 주입해야 함
선택지 B: payload 생성 시 UUID를 eventId로 넣는다
Outbox row id와 별개로 ProductRankingEvent.eventId를 생성합니다.
장점:
payload가 처음 저장될 때부터 완성됨
Kafka 메시지와 재빌드 payload가 동일한 구조
단점:
Outbox id와 eventId가 이원화됨
기존 event_handled 설계의 “OUTBOX_EVENTS id 재사용” 방향과 달라짐
eventId 중복/추적 책임이 payload 쪽으로 이동함
선택지 C: eventId는 payload에 넣지 않고, Outbox row id를 읽는 쪽에서 주입한다
PRODUCT_RANKING_EVENT payload에는 rankingEventType, productId, price, amount, occurredAt만 둡니다.
Kafka Relay 또는 RankingRebuildEventRepository가 OUTBOX_EVENTS.id를 eventId로 변환해 전달합니다.
장점:
Outbox id를 멱등성 키로 쓰는 정책을 유지
payload 저장 후 업데이트가 필요 없음
재빌드 시 원천 로그 id와 eventId가 일치
단점:
Kafka로 발행되는 최종 메시지 payload와 DB에 저장된 raw payload가 다를 수 있음
Relay가 payload에 eventId를 주입하거나, Kafka record key/header로 전달하는 정책이 필요함

다음 의사결정 질문입니다.
Kafka로 발행되는 ProductRankingEvent.eventId는 어디에 담을까요?
선택지 A: Relay가 payload JSON에 eventId를 주입해서 발행
DB raw payload에는 없지만, Kafka로 나갈 때는 최종 JSON에 eventId가 포함됩니다.
장점:
RankingKafkaConsumer는 payload만 파싱하면 됨
실시간 Consumer와 Rebuild Job이 같은 ProductRankingEvent 구조를 사용하기 쉬움
현재 ProductRankingEvent record 구조를 거의 유지 가능
단점:
DB에 저장된 raw payload와 Kafka 발행 payload가 달라짐
Relay가 특정 이벤트 타입의 payload를 조작해야 함
선택지 B: OUTBOX_EVENTS.id를 Kafka record key 또는 header로 전달
payload는 raw payload 그대로 발행하고, eventId는 Kafka metadata에서 읽습니다.
장점:
payload 변형이 없음
eventId가 메시지 메타데이터 역할이라는 점이 명확함
단점:
Consumer가 payload + header/key를 함께 읽어야 함
Rebuild Job은 DB id를 직접 주입하므로, 실시간 Consumer와 입력 조립 방식이 달라짐
현재 단순 payload 기반 Consumer보다 테스트/구현이 복잡해짐
선택지 C: Consumer가 Outbox id를 알 수 없으므로 Kafka 경로는 UUID, Rebuild 경로는 Outbox id 사용
이건 구조가 단순해 보일 수 있지만, 실시간 멱등성과 재빌드 멱등성 기준이 달라집니다.
장점:
Relay 변경이 적을 수 있음
단점:
같은 원천 이벤트의 식별자가 경로마다 달라짐
중복 방지와 추적성이 약해짐
앞선 “Outbox row id를 eventId로 사용” 결정과 어긋남

다음 의사결정 질문입니다.
modules/ranking-contract에 어디까지 넣을까요?
선택지 A: 순수 계약 DTO/Enum만 둔다
예:
ProductRankingEvent
RankingEventType
ProductRankingPayload
점수 정책과 Redis key 정책은 각 앱에 둡니다.
장점:
contract 모듈이 가볍고 의존성이 거의 없음
순환 의존 위험이 낮음
단점:
RankingKafkaConsumer, RankingRebuildJob, commerce-api가 key/date/score 정책을 중복 구현할 수 있음
“공통 비즈니스 로직 분리” 효과가 약함
선택지 B: 계약 DTO/Enum + 순수 정책까지 둔다
예:
ProductRankingEvent
RankingEventType
RankingScorePolicy
RankingKeyPolicy
장점:
실시간 Consumer, Rebuild Job, API 조회가 같은 key/date/score 정책을 재사용
Redis 모듈에 랭킹 스키마가 새지 않음
“공통되는 비즈니스 로직은 contract 모듈로”라는 방향과 잘 맞음
단점:
contract 모듈이 단순 DTO 이상이 됨
정책 변경 시 이 모듈을 참조하는 모든 앱 영향 범위가 생김
선택지 C: 계약 DTO/Enum + key/date 정책만 둔다
점수 정책은 streamer의 도메인 로직으로 남기고, Redis key/date 계산만 공유합니다.
장점:
API와 Streamer가 공유해야 하는 key/date 정책은 중복 제거
점수 계산은 랭킹 적재/재빌드 쪽 책임으로 유지
단점:
Rebuild Job과 Consumer가 같은 점수 정책을 써야 하므로 결국 streamer 내부에서만 공유됨
나중에 다른 모듈이 점수 계산을 필요로 하면 다시 이동해야 함

다음 의사결정 질문입니다.
commerce-api도 modules/ranking-contract를 의존하게 할까요?
현재 commerce-api는 랭킹 API 조회를 위해 Redis key를 알아야 합니다.
따라서 RankingKeyPolicy를 공통 모듈로 옮기면 API도 이 모듈을 참조할 수 있습니다.
선택지 A: commerce-api도 ranking-contract를 의존한다
장점:
API와 Streamer가 같은 ranking key/date 정책을 사용
ranking:all:{yyyyMMdd} 문자열 중복 제거
오늘 기준 상세 랭킹 조회도 같은 dateKey 정책 사용
단점:
API가 랭킹 계약 모듈에 의존하게 됨
계약 모듈 변경 시 API도 영향받음
선택지 B: commerce-api는 의존하지 않고 key 문자열만 자체 유지한다
장점:
API 의존성 증가를 피함
변경 범위가 작음
단점:
ranking:all:{yyyyMMdd} 정책이 API/Streamer에 중복됨
key 정책 변경 시 둘 중 하나가 누락될 수 있음
앞서 RankingKeyPolicy를 공용화한 효과가 줄어듦

다음 의사결정 질문입니다.
event-contract의 OutboxEventLog는 어떤 필드를 가져야 할까요?
재빌드 관점에서 필요한 최소 필드는 다음입니다.
id
eventType
payload
createdAt 또는 occurredAt
status
하지만 앞선 결정에 따르면 ranking event의 실제 발생 시각은 payload 내부 occurredAt을 사용합니다.
따라서 createdAt은 재빌드 조회 범위를 잡을 때 사용할지 여부가 중요합니다.
선택지 A: 최소 필드만 둔다
id
eventType
payload
장점:
계약이 가장 단순함
ranking payload 내부의 occurredAt만 기준으로 사용
단점:
DB 조회 범위 조건을 표현하기 어려움
INIT, COMPLETED 상태 필터를 공용 계약으로 나타내기 어려움
재빌드 대상 기간 조회 시 payload 파싱 전에는 날짜 필터링이 어려움
선택지 B: 조회 조건에 필요한 메타데이터까지 둔다
id
eventType
status
payload
createdAt
장점:
INIT, COMPLETED 필터와 기간 조회 조건을 명확히 표현 가능
OUTBOX_EVENTS.created_at 기준으로 후보를 좁힌 뒤 payload의 occurredAt으로 실제 ranking date를 계산할 수 있음
운영/디버깅에도 좋음
단점:
Outbox 테이블 메타데이터를 contract에 일부 노출함
계약이 A보다 조금 무거움
선택지 C: 재빌드 전용 조회 조건 객체와 결과 객체를 분리한다
예:
OutboxEventLogSearchCondition
eventType
statuses
createdFrom
createdTo

OutboxEventLog
id
eventType
status
payload
createdAt
장점:
조회 책임이 명확함
streamer의 RankingRebuildEventRepository 구현 계획을 문서화하기 좋음
나중에 페이징/배치 조회를 추가하기 쉬움
단점:
문서/계약이 가장 길어짐
이번 변경에는 살짝 과해 보일 수 있음

다음 의사결정 질문입니다.
문서에는 OUTBOX_EVENTS.created_at 조회 범위를 어떻게 잡을까요?
랭킹 날짜는 payload의 occurredAt 기준입니다. 그런데 DB에서 재빌드 후보를 찾을 때 payload 전체를 파싱해서 날짜 필터링하면 비효율적입니다. 그래서 보통은 createdAt으로 후보를 넉넉히 좁힌 뒤, payload의 occurredAt으로 실제 반영 날짜를 판단합니다.
선택지 A: 오늘/전일 createdAt만 조회
재빌드 대상이 오늘/전일이면, OUTBOX_EVENTS.created_at도 오늘/전일인 이벤트만 조회합니다.
장점:
조회 범위가 작고 단순함
구현이 쉬움
단점:
지연 기록/지연 발행/시간대 차이로 occurredAt은 전일인데 createdAt은 오늘 또는 그 반대인 이벤트 경계 처리가 약함
이벤트 발생일 기준 랭킹이라는 앞선 결정과 완전히 맞지는 않음
선택지 B: 오늘/전일 + 여유 버퍼 기간을 조회
예: 재빌드 대상이 오늘/전일이면 createdAt 기준 최근 3일 또는 4일을 조회한 뒤, payload occurredAt이 오늘/전일인 이벤트만 반영합니다.
장점:
이벤트 발생 시각 기준 랭킹 정책과 더 잘 맞음
자정 전후 지연 이벤트를 흡수할 수 있음
Redis TTL 2일보다 약간 긴 후보 조회로 복구 안정성이 올라감
단점:
조회량이 A보다 늘어남
버퍼 기간을 설정값/정책으로 둬야 함
선택지 C: createdAt으로 기간 필터링하지 않고 전체 조회 후 payload occurredAt으로 필터링
장점:
발생일 기준으로 가장 정확함
createdAt/occurredAt 차이를 신경 쓰지 않아도 됨
단점:
Outbox가 커지면 비현실적
운영 DB에 부담이 큼

좋습니다. 11차 결정은 B로 정리하겠습니다.
재빌드 대상 랭킹 날짜는 오늘/전일이다.
후보 OUTBOX_EVENTS 조회는 createdAt 기준 오늘/전일보다 넉넉한 버퍼 기간을 둔다.
기본 후보 조회 범위는 createdAt >= 재빌드 기준일 - 3일 정도로 문서화한다.
실제 랭킹 반영 여부는 payload 내부 occurredAt이 오늘/전일인지로 판단한다.
다음 의사결정 질문입니다.
RankingRebuildJob이 운영 Key를 교체할 때, 실시간 RankingKafkaConsumer와 충돌을 어떻게 다룰까요?
재빌드 흐름은 보통 이렇습니다.
1. ranking:rebuild:all:{yyyyMMdd} 임시 Key에 재계산
2. 완료 후 ranking:all:{yyyyMMdd} 운영 Key로 교체
   그런데 재빌드 중에도 RankingKafkaConsumer가 ranking:all:{yyyyMMdd}에 실시간 이벤트를 계속 반영할 수 있습니다.
   이 상태에서 임시 Key를 운영 Key로 덮어쓰면, 재빌드 시작 이후 실시간으로 들어온 점수가 날아갈 수 있습니다.
   선택지 A: 운영 절차상 RankingKafkaConsumer를 잠시 중단하고 재빌드한다
   장점:
   구현과 설명이 가장 단순함
   Key 교체 시 충돌 위험이 낮음
   단점:
   재빌드 동안 랭킹 반영 lag이 쌓임
   운영 절차 의존이 생김
   선택지 B: 재빌드 기준 시각을 잡고, 교체 후 기준 시각 이후 이벤트를 다시 replay한다
   장점:
   Consumer를 오래 멈추지 않아도 됨
   실시간 이벤트 유실을 줄일 수 있음
   단점:
   재빌드 기준 시각 이후 이벤트 replay 절차가 필요
   구현/운영 복잡도가 올라감
   선택지 C: 이번 설계 문서에서는 충돌 방지 절차를 명시하되 구현은 후속으로 둔다
   예: “운영 Key 교체 시 Consumer 일시 중단 또는 기준 시각 이후 replay 중 하나를 선택해야 한다”라고 리스크와 선택지를 남깁니다.
   장점:
   C→B 변경으로 Outbox 조회 구현은 포함하되, 운영 무중단 재빌드까지 과하게 확장하지 않음
   리스크를 숨기지 않음
   단점:
   완전 자동/무중단 재빌드 설계는 아직 아님

최종 결정: C
이번 설계 문서에서는 `OUTBOX_EVENTS` 조회 구현까지 B 방향으로 포함하되, 운영 Key 교체 중 `RankingKafkaConsumer`와의 충돌 방지는 후속 운영 절차로 둔다.

## Redis 랭킹 재빌드 원천 조회 최종 결정

기존 결정 C(재빌드 Job은 포트만 유지하고 실제 `OUTBOX_EVENTS` 조회 구현 보류)를 철회하고, 선택지 B로 변경한다.

- `RankingRebuildJob`은 `OUTBOX_EVENTS`를 원천 이벤트 로그로 사용한다.
- `modules/event-contract`와 `modules/ranking-contract`를 분리한다.
- `modules/event-contract`는 읽기 전용 `OutboxEventLog(id, eventType, status, payload, createdAt)` 계약을 제공한다.
- JPA Entity는 공용화하지 않는다. `commerce-api`는 기존 Outbox 쓰기 모델을 유지하고, `commerce-streamer`는 재빌드용 조회 구현에서 `OutboxEventLog` 계약으로 변환한다.
- `modules/ranking-contract`는 `ProductRankingEvent`, `RankingEventType`, `RankingScorePolicy`, `RankingKeyPolicy`를 제공한다.
- `commerce-api`와 `commerce-streamer`는 `modules/ranking-contract`를 의존해 같은 ranking key/date/score 정책을 사용한다.
- Outbox에는 랭킹 표준 이벤트를 `PRODUCT_RANKING_EVENT` 단일 `event_type`으로 저장한다.
- 실제 랭킹 행위는 payload 내부 `rankingEventType`(`VIEW`, `LIKE`, `ORDER`, `PRODUCT_DELETED`)으로 구분한다.
- DB에 저장되는 raw payload에는 `eventId`를 넣지 않는다.
- Kafka Relay와 `RankingRebuildEventRepository`가 `OUTBOX_EVENTS.id`를 `ProductRankingEvent.eventId`로 주입한다.
- 재빌드 조회 대상은 `event_type = PRODUCT_RANKING_EVENT`, `status in (INIT, COMPLETED)`이다. `FAILED` 상태는 기본 재빌드 대상에서 제외한다.
- 재빌드 후보는 `createdAt` 기준 최근 3일 버퍼로 조회하고, 실제 랭킹 반영 여부는 payload 내부 `occurredAt`이 오늘/전일인지로 판단한다.
- 운영 Key 교체 중 실시간 Consumer와의 충돌 방지는 이번 구현 범위에서 자동화하지 않는다. 후속 운영 절차에서 Consumer 일시 중단 또는 기준 시각 이후 이벤트 replay 중 하나를 선택해야 한다.
