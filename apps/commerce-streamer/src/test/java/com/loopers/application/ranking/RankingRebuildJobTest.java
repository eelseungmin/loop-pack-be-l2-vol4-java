package com.loopers.application.ranking;

import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingKeyPolicy;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.domain.ranking.RankingScorePolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RankingRebuildJobTest {

    @Test
    @DisplayName("오늘/전일 랭킹 이벤트를 재계산해 임시 랭킹에 적재하고 운영 랭킹으로 교체한다.")
    void rebuildTodayAndYesterday_ShouldRecalculateAndReplaceRankingKeys() {
        // given
        RankingRebuildEventRepository rebuildEventRepository = mock(RankingRebuildEventRepository.class);
        RankingRedisRepository rankingRedisRepository = mock(RankingRedisRepository.class);
        RankingRebuildJob rankingRebuildJob = new RankingRebuildJob(
            rebuildEventRepository,
            rankingRedisRepository,
            new RankingScorePolicy(),
            new RankingKeyPolicy()
        );
        LocalDate today = LocalDate.of(2026, 7, 14);
        LocalDateTime from = LocalDateTime.of(2026, 7, 13, 0, 0);
        LocalDateTime to = LocalDateTime.of(2026, 7, 15, 0, 0);
        LocalDateTime todayEventTime = LocalDateTime.of(2026, 7, 14, 10, 0);
        LocalDateTime yesterdayEventTime = LocalDateTime.of(2026, 7, 13, 10, 0);

        given(rebuildEventRepository.findEventsForRebuild(from, to))
            .willReturn(List.of(
                new ProductRankingEvent("event-1", RankingEventType.VIEW, 1L, BigDecimal.ZERO, 0, todayEventTime),
                new ProductRankingEvent("event-2", RankingEventType.ORDER, 2L, new BigDecimal("10000"), 2, yesterdayEventTime),
                new ProductRankingEvent("event-3", RankingEventType.PRODUCT_DELETED, 1L, BigDecimal.ZERO, 0, todayEventTime)
            ));

        // when
        rankingRebuildJob.rebuildTodayAndYesterday(today);

        // then
        verify(rankingRedisRepository).clearRebuildRanking("20260714");
        verify(rankingRedisRepository).clearRebuildRanking("20260713");
        verify(rankingRedisRepository).incrementRebuildScore("20260714", 1L, 0.1);
        verify(rankingRedisRepository).incrementRebuildScore("20260713", 2L, 0.6 * Math.log(20001));
        verify(rankingRedisRepository).removeProductFromRecentRebuildRankings(1L, todayEventTime);
        verify(rankingRedisRepository).replaceRankingWithRebuild("20260714");
        verify(rankingRedisRepository).replaceRankingWithRebuild("20260713");
    }
}
