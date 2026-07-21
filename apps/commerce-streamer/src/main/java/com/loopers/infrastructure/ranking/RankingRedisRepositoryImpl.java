package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingRedisRepository;
import com.loopers.application.ranking.RankingScoreEntry;
import com.loopers.domain.ranking.RankingKeyPolicy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

@Component
public class RankingRedisRepositoryImpl implements RankingRedisRepository {

    private static final Duration RANKING_TTL = Duration.ofDays(2);

    private final RedisTemplate<String, String> defaultRedisTemplate;
    private final RankingKeyPolicy rankingKeyPolicy;

    public RankingRedisRepositoryImpl(
        RedisTemplate<String, String> defaultRedisTemplate,
        RankingKeyPolicy rankingKeyPolicy
    ) {
        this.defaultRedisTemplate = defaultRedisTemplate;
        this.rankingKeyPolicy = rankingKeyPolicy;
    }

    @Override
    public boolean incrementScoreIfFirstHandled(String eventId, String dateKey, Long productId, double score) {
        String handledKey = rankingKeyPolicy.handledKey(dateKey);
        String rankingKey = rankingKeyPolicy.rankingKey(dateKey);

        Long addedCount = defaultRedisTemplate.opsForSet().add(handledKey, eventId);
        if (addedCount == null || addedCount == 0) {
            return false;
        }

        defaultRedisTemplate.opsForZSet().incrementScore(rankingKey, String.valueOf(productId), score);
        defaultRedisTemplate.expire(handledKey, RANKING_TTL);
        defaultRedisTemplate.expire(rankingKey, RANKING_TTL);
        return true;
    }

    @Override
    public void removeProductFromRecentRankings(Long productId, LocalDateTime deletedAt) {
        String productKey = String.valueOf(productId);
        defaultRedisTemplate.opsForZSet().remove(rankingKeyPolicy.rankingKey(rankingKeyPolicy.dateKey(deletedAt)), productKey);
        defaultRedisTemplate.opsForZSet().remove(rankingKeyPolicy.rankingKey(rankingKeyPolicy.dateKey(deletedAt.minusDays(1))), productKey);
    }

    @Override
    public void clearRebuildRanking(String dateKey) {
        defaultRedisTemplate.delete(rankingKeyPolicy.rebuildRankingKey(dateKey));
    }

    @Override
    public void incrementRebuildScore(String dateKey, Long productId, double score) {
        String rebuildKey = rankingKeyPolicy.rebuildRankingKey(dateKey);
        defaultRedisTemplate.opsForZSet().incrementScore(rebuildKey, String.valueOf(productId), score);
        defaultRedisTemplate.expire(rebuildKey, RANKING_TTL);
    }

    @Override
    public void removeProductFromRecentRebuildRankings(Long productId, LocalDateTime deletedAt) {
        String productKey = String.valueOf(productId);
        defaultRedisTemplate.opsForZSet().remove(rankingKeyPolicy.rebuildRankingKey(rankingKeyPolicy.dateKey(deletedAt)), productKey);
        defaultRedisTemplate.opsForZSet().remove(rankingKeyPolicy.rebuildRankingKey(rankingKeyPolicy.dateKey(deletedAt.minusDays(1))), productKey);
    }

    @Override
    public void replaceRankingWithRebuild(String dateKey) {
        String rankingKey = rankingKeyPolicy.rankingKey(dateKey);
        String rebuildKey = rankingKeyPolicy.rebuildRankingKey(dateKey);

        defaultRedisTemplate.delete(rankingKey);
        Boolean hasRebuildKey = defaultRedisTemplate.hasKey(rebuildKey);
        if (Boolean.TRUE.equals(hasRebuildKey)) {
            defaultRedisTemplate.rename(rebuildKey, rankingKey);
            defaultRedisTemplate.expire(rankingKey, RANKING_TTL);
        }
    }

    @Override
    public List<RankingScoreEntry> findTopRankings(String dateKey, int limit) {
        if (limit <= 0) {
            return List.of();
        }

        Set<ZSetOperations.TypedTuple<String>> rankings = defaultRedisTemplate.opsForZSet()
            .reverseRangeWithScores(rankingKeyPolicy.rankingKey(dateKey), 0, limit - 1L);
        if (rankings == null || rankings.isEmpty()) {
            return List.of();
        }

        return rankings.stream()
            .map(tuple -> new RankingScoreEntry(Long.valueOf(tuple.getValue()), tuple.getScore()))
            .toList();
    }

    @Override
    public void incrementCarryOverScore(String dateKey, Long productId, double score) {
        String rankingKey = rankingKeyPolicy.rankingKey(dateKey);
        defaultRedisTemplate.opsForZSet().incrementScore(rankingKey, String.valueOf(productId), score);
        defaultRedisTemplate.expire(rankingKey, RANKING_TTL);
    }

    @Override
    public boolean isCarryOverDone(String dateKey) {
        return Boolean.TRUE.equals(defaultRedisTemplate.hasKey(rankingKeyPolicy.carryOverDoneKey(dateKey)));
    }

    @Override
    public void markCarryOverDone(String dateKey) {
        defaultRedisTemplate.opsForValue().set(rankingKeyPolicy.carryOverDoneKey(dateKey), "1", RANKING_TTL);
    }
}
