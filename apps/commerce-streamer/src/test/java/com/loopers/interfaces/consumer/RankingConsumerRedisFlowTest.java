package com.loopers.interfaces.consumer;

import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class RankingConsumerRedisFlowTest {

    @Autowired
    private RankingKafkaConsumer rankingKafkaConsumer;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("랭킹 이벤트 처리 시 발생일 기준 Redis ZSET에 반영하고 같은 eventId는 중복 가산하지 않는다.")
    void rankingListener_ShouldAccumulateRedisRankingOnceByEventId() {
        // given
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-1",
            RankingEventType.ORDER,
            1L,
            new BigDecimal("10000"),
            2,
            LocalDateTime.of(2026, 7, 13, 23, 59, 59)
        );

        // when
        rankingKafkaConsumer.rankingListener(event, acknowledgment);
        rankingKafkaConsumer.rankingListener(event, acknowledgment);

        // then
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260713", "1"))
            .isEqualTo(0.6 * Math.log(20001));
        assertThat(defaultRedisTemplate.opsForZSet().score("ranking:all:20260714", "1")).isNull();
        verify(acknowledgment, times(2)).acknowledge();
    }
}
