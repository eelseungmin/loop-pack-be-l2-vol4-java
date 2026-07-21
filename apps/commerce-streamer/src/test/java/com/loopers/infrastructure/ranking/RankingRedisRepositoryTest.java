package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingRedisRepository;
import com.loopers.application.ranking.RankingScoreEntry;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class RankingRedisRepositoryTest {

    @Autowired
    private RankingRedisRepository rankingRedisRepository;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("최초 처리 이벤트만 랭킹 점수에 반영하고 랭킹 키와 처리 이력 키에 2일 TTL을 설정한다.")
    void incrementScoreIfFirstHandled_ShouldIncreaseOnlyFirstEventAndSetTtl() {
        // given
        String dateKey = "20260714";
        String eventId = "event-1";
        Long productId = 1L;

        // when
        boolean firstResult = rankingRedisRepository.incrementScoreIfFirstHandled(eventId, dateKey, productId, 0.1);
        boolean secondResult = rankingRedisRepository.incrementScoreIfFirstHandled(eventId, dateKey, productId, 0.1);

        // then
        assertThat(firstResult).isTrue();
        assertThat(secondResult).isFalse();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1")).isEqualTo(0.1);
        assertThat(defaultRedisTemplate.getExpire("ranking:all:20260714", TimeUnit.SECONDS)).isBetween(172700L, 172800L);
        assertThat(defaultRedisTemplate.getExpire("ranking:handled:20260714", TimeUnit.SECONDS)).isBetween(172700L, 172800L);
    }

    @Test
    @DisplayName("상품 삭제 이벤트가 발생하면 삭제일과 전일 랭킹에서 해당 상품을 제거한다.")
    void removeProductFromRecentRankings_ShouldRemoveProductFromDeletedDateAndPreviousDate() {
        // given
        Long productId = 1L;
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "1", 10.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260713", "1", 10.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260712", "1", 10.0);

        // when
        rankingRedisRepository.removeProductFromRecentRankings(
            productId,
            LocalDateTime.of(2026, 7, 14, 12, 0)
        );

        // then
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1")).isNull();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260713", "1")).isNull();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260712", "1")).isEqualTo(10.0);
    }

    @Test
    @DisplayName("재빌드 임시 랭킹을 운영 랭킹으로 교체한다.")
    void replaceRankingWithRebuild_ShouldReplaceOperationalRanking() {
        // given
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "1", 99.0);
        rankingRedisRepository.incrementRebuildScore("20260714", 2L, 10.0);

        // when
        rankingRedisRepository.replaceRankingWithRebuild("20260714");

        // then
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1")).isNull();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "2")).isEqualTo(10.0);
        assertThat(defaultRedisTemplate.hasKey("ranking:rebuild:all:20260714")).isFalse();
        assertThat(defaultRedisTemplate.getExpire("ranking:all:20260714", TimeUnit.SECONDS)).isBetween(172700L, 172800L);
    }

    @Test
    @DisplayName("상품 삭제 이벤트가 발생하면 삭제일과 전일 재빌드 랭킹에서 해당 상품을 제거한다.")
    void removeProductFromRecentRebuildRankings_ShouldRemoveProductFromDeletedDateAndPreviousDate() {
        // given
        Long productId = 1L;
        rankingRedisRepository.incrementRebuildScore("20260714", productId, 10.0);
        rankingRedisRepository.incrementRebuildScore("20260713", productId, 10.0);
        rankingRedisRepository.incrementRebuildScore("20260712", productId, 10.0);

        // when
        rankingRedisRepository.removeProductFromRecentRebuildRankings(
            productId,
            LocalDateTime.of(2026, 7, 14, 12, 0)
        );

        // then
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:rebuild:all:20260714", "1")).isNull();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:rebuild:all:20260713", "1")).isNull();
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:rebuild:all:20260712", "1")).isEqualTo(10.0);
    }

    @Test
    @DisplayName("전일 랭킹 Top N을 조회하고 carry over 점수를 오늘 랭킹에 누적하며 done key에 2일 TTL을 설정한다.")
    void carryOverRanking_ShouldReadTopRankingsAndMarkDone() {
        // given
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260713", "1", 30.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260713", "2", 20.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260713", "3", 10.0);

        // when
        List<RankingScoreEntry> topRankings = rankingRedisRepository.findTopRankings("20260713", 2);
        rankingRedisRepository.incrementCarryOverScore("20260714", 1L, 3.0);
        rankingRedisRepository.markCarryOverDone("20260714");

        // then
        assertThat(topRankings)
            .extracting(RankingScoreEntry::productId, RankingScoreEntry::score)
            .containsExactly(
                org.assertj.core.groups.Tuple.tuple(1L, 30.0),
                org.assertj.core.groups.Tuple.tuple(2L, 20.0)
            );
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1")).isEqualTo(3.0);
        assertThat(defaultRedisTemplate.getExpire("ranking:all:20260714", TimeUnit.SECONDS)).isBetween(172700L, 172800L);
        assertThat(rankingRedisRepository.isCarryOverDone("20260714")).isTrue();
        assertThat(defaultRedisTemplate.getExpire("ranking:carry-over:done:20260714", TimeUnit.SECONDS)).isBetween(172700L, 172800L);
    }
}
