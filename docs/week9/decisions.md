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
상품 삭제 트랜잭션 안에서 OUTBOX_EVENTS에 PRODUCT_DELETED 이벤트 저장
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