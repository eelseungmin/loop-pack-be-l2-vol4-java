package com.loopers.interfaces.consumer;

import com.loopers.application.ranking.RankingRedisRepository;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.domain.ranking.RankingScorePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RankingKafkaConsumerTest {

    @Test
    @DisplayName("조회 이벤트를 수신하면 발생일 기준 랭킹 점수를 Redis에 반영하고 Ack한다.")
    void rankingListener_WhenViewEvent_ShouldIncrementRankingAndAcknowledge() {
        // given
        RankingRedisRepository rankingRedisRepository = mock(RankingRedisRepository.class);
        RankingKafkaConsumer rankingKafkaConsumer = new RankingKafkaConsumer(
            new RankingScorePolicy(),
            rankingRedisRepository
        );
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-1",
            RankingEventType.VIEW,
            1L,
            BigDecimal.ZERO,
            0,
            LocalDateTime.of(2026, 7, 14, 23, 59, 59)
        );
        when(rankingRedisRepository.incrementScoreIfFirstHandled("event-1", "20260714", 1L, 0.1))
            .thenReturn(true);

        // when
        rankingKafkaConsumer.rankingListener(event, acknowledgment);

        // then
        verify(rankingRedisRepository).incrementScoreIfFirstHandled("event-1", "20260714", 1L, 0.1);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("Redis 반영에 실패하면 Ack하지 않는다.")
    void rankingListener_WhenRedisFails_ShouldNotAcknowledge() {
        // given
        RankingRedisRepository rankingRedisRepository = mock(RankingRedisRepository.class);
        RankingKafkaConsumer rankingKafkaConsumer = new RankingKafkaConsumer(
            new RankingScorePolicy(),
            rankingRedisRepository
        );
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-1",
            RankingEventType.VIEW,
            1L,
            BigDecimal.ZERO,
            0,
            LocalDateTime.of(2026, 7, 14, 23, 59, 59)
        );
        when(rankingRedisRepository.incrementScoreIfFirstHandled("event-1", "20260714", 1L, 0.1))
            .thenThrow(new IllegalStateException("redis error"));

        // when
        try {
            rankingKafkaConsumer.rankingListener(event, acknowledgment);
        } catch (IllegalStateException ignored) {
        }

        // then
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    @DisplayName("상품 삭제 이벤트를 수신하면 최근 랭킹에서 상품을 제거하고 Ack한다.")
    void rankingListener_WhenProductDeletedEvent_ShouldRemoveProductFromRecentRankingsAndAcknowledge() {
        // given
        RankingRedisRepository rankingRedisRepository = mock(RankingRedisRepository.class);
        RankingKafkaConsumer rankingKafkaConsumer = new RankingKafkaConsumer(
            new RankingScorePolicy(),
            rankingRedisRepository
        );
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        LocalDateTime deletedAt = LocalDateTime.of(2026, 7, 14, 12, 0);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-2",
            RankingEventType.PRODUCT_DELETED,
            1L,
            BigDecimal.ZERO,
            0,
            deletedAt
        );

        // when
        rankingKafkaConsumer.rankingListener(event, acknowledgment);

        // then
        verify(rankingRedisRepository).removeProductFromRecentRankings(1L, deletedAt);
        verify(rankingRedisRepository, never()).incrementScoreIfFirstHandled("event-2", "20260714", 1L, 0.0);
        verify(acknowledgment).acknowledge();
    }
}
