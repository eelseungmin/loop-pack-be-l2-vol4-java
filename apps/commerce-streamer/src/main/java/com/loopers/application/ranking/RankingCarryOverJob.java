package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingKeyPolicy;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class RankingCarryOverJob {

    private static final int CARRY_OVER_LIMIT = 1_000;
    private static final double CARRY_OVER_RATE = 0.1;
    private static final LocalTime CARRY_OVER_CUTOFF = LocalTime.of(0, 10);

    private final RankingRedisRepository rankingRedisRepository;
    private final RankingCarryOverProductRepository productRepository;
    private final RankingKeyPolicy rankingKeyPolicy;

    public RankingCarryOverJob(
        RankingRedisRepository rankingRedisRepository,
        RankingCarryOverProductRepository productRepository,
        RankingKeyPolicy rankingKeyPolicy
    ) {
        this.rankingRedisRepository = rankingRedisRepository;
        this.productRepository = productRepository;
        this.rankingKeyPolicy = rankingKeyPolicy;
    }

    public void carryOver(LocalDateTime now) {
        if (now.toLocalTime().isAfter(CARRY_OVER_CUTOFF)) {
            return;
        }

        LocalDate today = now.toLocalDate();
        String todayKey = rankingKeyPolicy.dateKey(today.atStartOfDay());
        if (rankingRedisRepository.isCarryOverDone(todayKey)) {
            return;
        }

        String yesterdayKey = rankingKeyPolicy.dateKey(today.minusDays(1).atStartOfDay());
        List<RankingScoreEntry> yesterdayRankings = rankingRedisRepository.findTopRankings(yesterdayKey, CARRY_OVER_LIMIT);
        if (yesterdayRankings.isEmpty()) {
            rankingRedisRepository.markCarryOverDone(todayKey);
            return;
        }

        List<Long> productIds = yesterdayRankings.stream()
            .map(RankingScoreEntry::productId)
            .toList();
        Set<Long> availableProductIds = new HashSet<>(productRepository.findAvailableProductIds(productIds));

        for (RankingScoreEntry ranking : yesterdayRankings) {
            if (!availableProductIds.contains(ranking.productId())) {
                continue;
            }
            rankingRedisRepository.incrementCarryOverScore(
                todayKey,
                ranking.productId(),
                ranking.score() * CARRY_OVER_RATE
            );
        }

        rankingRedisRepository.markCarryOverDone(todayKey);
    }
}
