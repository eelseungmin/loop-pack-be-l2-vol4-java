package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingEntry;
import com.loopers.application.ranking.RankingRedisRepository;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
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
    @DisplayName("랭킹 ZSET을 점수 내림차순으로 1-based 페이지 조회한다.")
    void findRankings_ShouldReturnRankingEntriesByScoreDesc() {
        // given
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "1", 10.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "2", 9.0);

        // when
        List<RankingEntry> firstPage = rankingRedisRepository.findRankings("20260714", 1, 1);
        List<RankingEntry> secondPage = rankingRedisRepository.findRankings("20260714", 2, 1);

        // then
        assertThat(firstPage).containsExactly(new RankingEntry(1L, 1, 10.0));
        assertThat(secondPage).containsExactly(new RankingEntry(2L, 2, 9.0));
        assertThat(rankingRedisRepository.count("20260714")).isEqualTo(2L);
    }

    @Test
    @DisplayName("상품 ID로 특정 상품의 랭킹 정보를 조회한다.")
    void findProductRanking_ShouldReturnProductRankingInfo() {
        // given
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "1", 10.0);
        defaultRedisTemplate.opsForZSet().add("ranking:all:20260714", "2", 9.0);

        // when
        var rankingInfo = rankingRedisRepository.findProductRanking("20260714", 2L);

        // then
        assertThat(rankingInfo).isPresent();
        assertThat(rankingInfo.get().date()).isEqualTo("20260714");
        assertThat(rankingInfo.get().rank()).isEqualTo(2);
        assertThat(rankingInfo.get().score()).isEqualTo(9.0);
    }
}
