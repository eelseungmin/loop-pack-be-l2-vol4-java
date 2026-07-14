package com.loopers.application.queue;

import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class QueueSchedulerIntegrationTest {

    @Autowired
    private QueueScheduler queueScheduler;

    @Autowired
    private QueueRepository queueRepository;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("스케줄러 동작 시 waiting 큐의 맨 앞 N명이 active 토큰을 발급받고 waiting에서 제거된다.")
    void promoteWaitingUsers_shouldPromoteFirstNUsers() {
        // given
        for (long i = 1; i <= 25; i++) {
            queueRepository.enter(i, System.currentTimeMillis() + i);
        }
        assertThat(queueRepository.getWaitingCount()).isEqualTo(25L);

        // when
        queueScheduler.promoteWaitingUsers();

        // then
        for (long i = 1; i <= 21; i++) {
            Optional<String> token = queueRepository.getActiveToken(i);
            assertThat(token).isPresent();
        }
        assertThat(queueRepository.getWaitingCount()).isEqualTo(4L);
        assertThat(queueRepository.getRank(22L)).isPresent();
    }

    @Test
    @DisplayName("발급된 active 토큰은 5분(300초)의 TTL을 가진다.")
    void promoteWaitingUsers_shouldSetCorrectTtl() {
        // given
        queueRepository.enter(1L, System.currentTimeMillis());

        // when
        queueScheduler.promoteWaitingUsers();

        // then
        Optional<String> token = queueRepository.getActiveToken(1L);
        assertThat(token).isPresent();

        Long expire = defaultRedisTemplate.getExpire("queue:active:1", TimeUnit.SECONDS);
        assertThat(expire).isNotNull();
        assertThat(expire).isBetween(290L, 300L);
    }

    @Test
    @DisplayName("각 사용자의 Active 토큰은 독립적인 만료 시간(TTL)을 가진다.")
    void makeActive_shouldHaveIndependentTtlForUsers() {
        // given
        queueRepository.makeActive(101L, "token-101", 100);
        queueRepository.makeActive(102L, "token-102", 300);

        // then
        Long expire1 = defaultRedisTemplate.getExpire("queue:active:101", TimeUnit.SECONDS);
        Long expire2 = defaultRedisTemplate.getExpire("queue:active:102", TimeUnit.SECONDS);

        assertThat(expire1).isNotNull().isBetween(90L, 100L);
        assertThat(expire2).isNotNull().isBetween(290L, 300L);
        assertThat(expire1).isNotEqualTo(expire2);
    }
}
