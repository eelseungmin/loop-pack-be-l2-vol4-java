package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingEventType;
import com.loopers.domain.ranking.RankingScorePolicy;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class RankingRebuildJob {

    private static final DateTimeFormatter DATE_KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final RankingRebuildEventRepository rankingRebuildEventRepository;
    private final RankingRedisRepository rankingRedisRepository;
    private final RankingScorePolicy rankingScorePolicy;

    public RankingRebuildJob(
        RankingRebuildEventRepository rankingRebuildEventRepository,
        RankingRedisRepository rankingRedisRepository,
        RankingScorePolicy rankingScorePolicy
    ) {
        this.rankingRebuildEventRepository = rankingRebuildEventRepository;
        this.rankingRedisRepository = rankingRedisRepository;
        this.rankingScorePolicy = rankingScorePolicy;
    }

    public void rebuildTodayAndYesterday(LocalDate today) {
        LocalDate yesterday = today.minusDays(1);
        String todayKey = today.format(DATE_KEY_FORMATTER);
        String yesterdayKey = yesterday.format(DATE_KEY_FORMATTER);
        LocalDateTime from = yesterday.atStartOfDay();
        LocalDateTime to = today.plusDays(1).atStartOfDay();

        rankingRedisRepository.clearRebuildRanking(todayKey);
        rankingRedisRepository.clearRebuildRanking(yesterdayKey);

        List<RankingRebuildEvent> events = rankingRebuildEventRepository.findEventsForRebuild(from, to);
        for (RankingRebuildEvent event : events) {
            if (event.eventType() == RankingEventType.PRODUCT_DELETED) {
                rankingRedisRepository.removeProductFromRecentRebuildRankings(event.productId(), event.occurredAt());
                continue;
            }

            String dateKey = rankingScorePolicy.dateKey(event.occurredAt());
            double score = rankingScorePolicy.calculateScore(event.eventType(), event.price(), event.amount());
            rankingRedisRepository.incrementRebuildScore(dateKey, event.productId(), score);
        }

        rankingRedisRepository.replaceRankingWithRebuild(todayKey);
        rankingRedisRepository.replaceRankingWithRebuild(yesterdayKey);
    }
}
