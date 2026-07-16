package com.loopers.domain.ranking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RankingContractTest {

    private final RankingScorePolicy rankingScorePolicy = new RankingScorePolicy();
    private final RankingKeyPolicy rankingKeyPolicy = new RankingKeyPolicy();

    @Test
    @DisplayName("랭킹 표준 이벤트는 OUTBOX id 기반 eventId와 rankingEventType을 가진다.")
    void productRankingEvent_ShouldExposeEventIdAndRankingEventType() {
        // given
        ProductRankingEvent event = new ProductRankingEvent(
            "1",
            RankingEventType.ORDER,
            10L,
            BigDecimal.valueOf(10_000),
            2,
            LocalDateTime.of(2026, 7, 14, 10, 0)
        );

        // then
        assertThat(event.eventId()).isEqualTo("1");
        assertThat(event.rankingEventType()).isEqualTo(RankingEventType.ORDER);
        assertThat(event.eventType()).isEqualTo(RankingEventType.ORDER);
    }

    @Test
    @DisplayName("조회/좋아요/주문 이벤트의 랭킹 점수를 계산한다.")
    void calculateScore_ShouldReturnWeightedScoreByEventType() {
        assertThat(rankingScorePolicy.calculateScore(RankingEventType.VIEW, BigDecimal.ZERO, 0))
            .isEqualTo(0.1);
        assertThat(rankingScorePolicy.calculateScore(RankingEventType.LIKE, BigDecimal.ZERO, 0))
            .isEqualTo(0.2);
        assertThat(rankingScorePolicy.calculateScore(RankingEventType.ORDER, BigDecimal.valueOf(10_000), 2))
            .isEqualTo(0.6 * Math.log(20_001));
    }

    @Test
    @DisplayName("상품 삭제 이벤트는 점수를 계산할 수 없다.")
    void calculateScore_WhenProductDeleted_ShouldThrowException() {
        assertThatThrownBy(() -> rankingScorePolicy.calculateScore(
            RankingEventType.PRODUCT_DELETED,
            BigDecimal.ZERO,
            0
        )).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("랭킹 날짜와 Redis Key를 같은 정책에서 계산한다.")
    void rankingKeyPolicy_ShouldCalculateDateAndKeys() {
        // given
        LocalDateTime occurredAt = LocalDateTime.of(2026, 7, 14, 23, 59, 59);

        // when
        String dateKey = rankingKeyPolicy.dateKey(occurredAt);

        // then
        assertThat(dateKey).isEqualTo("20260714");
        assertThat(rankingKeyPolicy.rankingKey(dateKey)).isEqualTo("ranking:all:20260714");
        assertThat(rankingKeyPolicy.handledKey(dateKey)).isEqualTo("ranking:handled:20260714");
        assertThat(rankingKeyPolicy.rebuildRankingKey(dateKey)).isEqualTo("ranking:rebuild:all:20260714");
    }
}
