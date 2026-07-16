package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingEntry;
import com.loopers.application.ranking.ProductRankingInfo;
import com.loopers.application.ranking.RankingRedisRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Component
public class RankingRedisRepositoryImpl implements RankingRedisRepository {

    private static final String RANKING_KEY_PREFIX = "ranking:all:";

    private final RedisTemplate<String, String> defaultRedisTemplate;

    public RankingRedisRepositoryImpl(RedisTemplate<String, String> defaultRedisTemplate) {
        this.defaultRedisTemplate = defaultRedisTemplate;
    }

    @Override
    public List<RankingEntry> findRankings(String dateKey, int page, int size) {
        long start = (long) Math.max(page - 1, 0) * size;
        long end = start + size - 1;
        Set<ZSetOperations.TypedTuple<String>> tuples = defaultRedisTemplate.opsForZSet()
            .reverseRangeWithScores(RANKING_KEY_PREFIX + dateKey, start, end);

        if (tuples == null || tuples.isEmpty()) {
            return List.of();
        }

        List<RankingEntry> entries = new ArrayList<>();
        long rank = start + 1;
        for (ZSetOperations.TypedTuple<String> tuple : tuples) {
            if (tuple.getValue() == null || tuple.getScore() == null) {
                continue;
            }
            entries.add(new RankingEntry(Long.valueOf(tuple.getValue()), rank, tuple.getScore()));
            rank++;
        }
        return entries;
    }

    @Override
    public long count(String dateKey) {
        Long size = defaultRedisTemplate.opsForZSet().size(RANKING_KEY_PREFIX + dateKey);
        return size == null ? 0L : size;
    }

    @Override
    public Optional<ProductRankingInfo> findProductRanking(String dateKey, Long productId) {
        String rankingKey = RANKING_KEY_PREFIX + dateKey;
        String productKey = String.valueOf(productId);
        Long rank = defaultRedisTemplate.opsForZSet().reverseRank(rankingKey, productKey);
        if (rank == null) {
            return Optional.empty();
        }
        Double score = defaultRedisTemplate.opsForZSet().score(rankingKey, productKey);
        if (score == null) {
            return Optional.empty();
        }
        return Optional.of(new ProductRankingInfo(dateKey, rank + 1, score));
    }
}
