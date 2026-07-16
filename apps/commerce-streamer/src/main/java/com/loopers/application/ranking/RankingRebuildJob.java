package com.loopers.application.ranking;

import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingKeyPolicy;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.domain.ranking.RankingScorePolicy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class RankingRebuildJob {

    private final RankingRebuildEventRepository rankingRebuildEventRepository;
    private final RankingRedisRepository rankingRedisRepository;
    private final RankingScorePolicy rankingScorePolicy;
    private final RankingKeyPolicy rankingKeyPolicy;

    public RankingRebuildJob(
        RankingRebuildEventRepository rankingRebuildEventRepository,
        RankingRedisRepository rankingRedisRepository,
        RankingScorePolicy rankingScorePolicy,
        RankingKeyPolicy rankingKeyPolicy
    ) {
        this.rankingRebuildEventRepository = rankingRebuildEventRepository;
        this.rankingRedisRepository = rankingRedisRepository;
        this.rankingScorePolicy = rankingScorePolicy;
        this.rankingKeyPolicy = rankingKeyPolicy;
    }

    public void rebuildTodayAndYesterday(LocalDate today) {
        LocalDate yesterday = today.minusDays(1);
        String todayKey = rankingKeyPolicy.dateKey(today.atStartOfDay());
        String yesterdayKey = rankingKeyPolicy.dateKey(yesterday.atStartOfDay());
        LocalDateTime from = yesterday.atStartOfDay();
        LocalDateTime to = today.plusDays(1).atStartOfDay();

        rankingRedisRepository.clearRebuildRanking(todayKey);
        rankingRedisRepository.clearRebuildRanking(yesterdayKey);

        List<ProductRankingEvent> events = rankingRebuildEventRepository.findEventsForRebuild(from, to);
        for (ProductRankingEvent event : events) {
            if (event.eventType() == RankingEventType.PRODUCT_DELETED) {
                rankingRedisRepository.removeProductFromRecentRebuildRankings(event.productId(), event.occurredAt());
                continue;
            }

            String dateKey = rankingKeyPolicy.dateKey(event.occurredAt());
            double score = rankingScorePolicy.calculateScore(event.eventType(), event.price(), event.amount());
            rankingRedisRepository.incrementRebuildScore(dateKey, event.productId(), score);
        }

        rankingRedisRepository.replaceRankingWithRebuild(todayKey);
        rankingRedisRepository.replaceRankingWithRebuild(yesterdayKey);
    }
}
