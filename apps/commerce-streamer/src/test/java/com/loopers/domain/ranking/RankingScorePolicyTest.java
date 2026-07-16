package com.loopers.domain.ranking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class RankingScorePolicyTest {

    private final RankingScorePolicy rankingScorePolicy = new RankingScorePolicy();

    @Test
    @DisplayName("조회 이벤트 점수는 0.1이다.")
    void calculateScore_WhenViewEvent_ShouldReturnViewScore() {
        // when
        double score = rankingScorePolicy.calculateScore(RankingEventType.VIEW, BigDecimal.ZERO, 0);

        // then
        assertThat(score).isEqualTo(0.1);
    }

    @Test
    @DisplayName("좋아요 이벤트 점수는 0.2이다.")
    void calculateScore_WhenLikeEvent_ShouldReturnLikeScore() {
        // when
        double score = rankingScorePolicy.calculateScore(RankingEventType.LIKE, BigDecimal.ZERO, 0);

        // then
        assertThat(score).isEqualTo(0.2);
    }

    @Test
    @DisplayName("주문 이벤트 점수는 0.6 * log(price * amount + 1)이다.")
    void calculateScore_WhenOrderEvent_ShouldReturnNormalizedOrderScore() {
        // given
        BigDecimal price = new BigDecimal("10000");
        int amount = 2;

        // when
        double score = rankingScorePolicy.calculateScore(RankingEventType.ORDER, price, amount);

        // then
        assertThat(score).isEqualTo(0.6 * Math.log(20001));
    }

    @Test
    @DisplayName("랭킹 날짜 키는 이벤트 발생 시각 기준으로 계산한다.")
    void dateKey_ShouldUseOccurredAt() {
        // given
        LocalDateTime occurredAt = LocalDateTime.of(2026, 7, 14, 23, 59, 59);

        // when
        String dateKey = rankingScorePolicy.dateKey(occurredAt);

        // then
        assertThat(dateKey).isEqualTo("20260714");
    }
}
