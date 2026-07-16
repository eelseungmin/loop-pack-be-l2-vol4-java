package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingRedisRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class RankingRedisRepositoryImpl implements RankingRedisRepository {

    private static final String RANKING_KEY_PREFIX = "ranking:all:";
    private static final String REBUILD_RANKING_KEY_PREFIX = "ranking:rebuild:all:";
    private static final String HANDLED_KEY_PREFIX = "ranking:handled:";
    private static final Duration RANKING_TTL = Duration.ofDays(2);
    private static final DateTimeFormatter DATE_KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final RedisTemplate<String, String> defaultRedisTemplate;

    public RankingRedisRepositoryImpl(RedisTemplate<String, String> defaultRedisTemplate) {
        this.defaultRedisTemplate = defaultRedisTemplate;
    }

    @Override
    public boolean incrementScoreIfFirstHandled(String eventId, String dateKey, Long productId, double score) {
        String handledKey = HANDLED_KEY_PREFIX + dateKey;
        String rankingKey = RANKING_KEY_PREFIX + dateKey;

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
        defaultRedisTemplate.opsForZSet().remove(RANKING_KEY_PREFIX + deletedAt.format(DATE_KEY_FORMATTER), productKey);
        defaultRedisTemplate.opsForZSet().remove(RANKING_KEY_PREFIX + deletedAt.minusDays(1).format(DATE_KEY_FORMATTER), productKey);
    }

    @Override
    public void clearRebuildRanking(String dateKey) {
        defaultRedisTemplate.delete(REBUILD_RANKING_KEY_PREFIX + dateKey);
    }

    @Override
    public void incrementRebuildScore(String dateKey, Long productId, double score) {
        String rebuildKey = REBUILD_RANKING_KEY_PREFIX + dateKey;
        defaultRedisTemplate.opsForZSet().incrementScore(rebuildKey, String.valueOf(productId), score);
        defaultRedisTemplate.expire(rebuildKey, RANKING_TTL);
    }

    @Override
    public void removeProductFromRecentRebuildRankings(Long productId, LocalDateTime deletedAt) {
        String productKey = String.valueOf(productId);
        defaultRedisTemplate.opsForZSet().remove(REBUILD_RANKING_KEY_PREFIX + deletedAt.format(DATE_KEY_FORMATTER), productKey);
        defaultRedisTemplate.opsForZSet().remove(REBUILD_RANKING_KEY_PREFIX + deletedAt.minusDays(1).format(DATE_KEY_FORMATTER), productKey);
    }

    @Override
    public void replaceRankingWithRebuild(String dateKey) {
        String rankingKey = RANKING_KEY_PREFIX + dateKey;
        String rebuildKey = REBUILD_RANKING_KEY_PREFIX + dateKey;

        defaultRedisTemplate.delete(rankingKey);
        Boolean hasRebuildKey = defaultRedisTemplate.hasKey(rebuildKey);
        if (Boolean.TRUE.equals(hasRebuildKey)) {
            defaultRedisTemplate.rename(rebuildKey, rankingKey);
            defaultRedisTemplate.expire(rankingKey, RANKING_TTL);
        }
    }
}
