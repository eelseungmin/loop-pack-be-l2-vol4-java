package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingKeyPolicy;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class RankingColdStartCarryOverScaleTest {

    private static final int PRODUCT_COUNT = 100_000;
    private static final int USER_COUNT = 20_000;
    private static final int CARRY_OVER_LIMIT = 1_000;

    @Autowired
    private RankingRedisRepository rankingRedisRepository;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    private RankingCarryOverJob rankingCarryOverJob;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
        rankingCarryOverJob = new RankingCarryOverJob(
            rankingRedisRepository,
            productIds -> productIds,
            new RankingKeyPolicy()
        );
    }

    @Test
    @DisplayName("상품 10만 건, 유저 2만 명 기준 전일 랭킹이 있으면 오늘 랭킹 콜드스타트를 Top 1,000 carry over로 완화한다.")
    void carryOver_ShouldWarmUpTodayRankingFromYesterdayTopRankings() {
        // given
        seedYesterdayRankingWithDummyInteractions("20260713");
        assertThat(defaultRedisTemplate.opsForZSet().zCard("ranking:all:20260714")).isZero();

        // when
        rankingCarryOverJob.carryOver(LocalDateTime.of(2026, 7, 14, 0, 5));

        // then
        assertThat(defaultRedisTemplate.opsForZSet().zCard("ranking:all:20260714"))
            .isEqualTo(CARRY_OVER_LIMIT);
        assertThat(defaultRedisTemplate.opsForZSet().reverseRank("ranking:all:20260714", "1"))
            .isEqualTo(0L);
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1"))
            .isEqualTo(yesterdayScore(1L) * 0.1);
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1000"))
            .isEqualTo(yesterdayScore(1000L) * 0.1);
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1001"))
            .isNull();
        assertThat(defaultRedisTemplate.getExpire("ranking:all:20260714", TimeUnit.SECONDS))
            .isBetween(172700L, 172800L);
        assertThat(defaultRedisTemplate.hasKey("ranking:carry-over:done:20260714"))
            .isTrue();

        // when
        rankingCarryOverJob.carryOver(LocalDateTime.of(2026, 7, 14, 0, 6));

        // then
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1"))
            .isEqualTo(yesterdayScore(1L) * 0.1);
        assertThat(defaultRedisTemplate.opsForZSet().zCard("ranking:all:20260714"))
            .isEqualTo(CARRY_OVER_LIMIT);
    }

    private void seedYesterdayRankingWithDummyInteractions(String dateKey) {
        String rankingKey = "ranking:all:" + dateKey;
        RedisSerializer<String> serializer = defaultRedisTemplate.getStringSerializer();
        byte[] key = serializer.serialize(rankingKey);

        defaultRedisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (long productId = 1; productId <= PRODUCT_COUNT; productId++) {
                long userId = ((productId - 1) % USER_COUNT) + 1;
                connection.zSetCommands().zAdd(
                    key,
                    yesterdayScore(productId, userId),
                    serializer.serialize(String.valueOf(productId))
                );
            }
            return null;
        });
    }

    private double yesterdayScore(long productId) {
        long userId = ((productId - 1) % USER_COUNT) + 1;
        return yesterdayScore(productId, userId);
    }

    private double yesterdayScore(long productId, long userId) {
        double deterministicInteractionWeight = (USER_COUNT - userId) / 1_000_000.0;
        return (PRODUCT_COUNT - productId + 1) + deterministicInteractionWeight;
    }
}
