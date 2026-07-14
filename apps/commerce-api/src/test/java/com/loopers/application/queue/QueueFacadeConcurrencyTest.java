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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class QueueFacadeConcurrencyTest {

    @Autowired
    private QueueFacade queueFacade;

    @Autowired
    private QueueRepository queueRepository;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("다수의 유저가 동시에 대기열 진입을 시도할 때 중복 없이 순서대로 ZSet에 적재되어야 한다.")
    void enterQueue_ConcurrencyTest() throws InterruptedException {
        // given
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount);

        // when
        for (int i = 1; i <= threadCount; i++) {
            final long userId = i;
            executorService.submit(() -> {
                try {
                    queueFacade.enterQueue(userId);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        // then
        assertThat(queueRepository.getWaitingCount()).isEqualTo((long) threadCount);
    }

    @Test
    @DisplayName("유저들이 대기열에 진입하는 동시에 스케줄러가 유저를 Active 상태로 이동시키는 경합 상황에서도, 한 유저가 WAITING과 ACTIVE 양쪽에 동시에 존재하는 중복 상태가 발생하지 않는다.")
    void enterQueueAndScheduler_ConcurrencyTest() throws InterruptedException {
        // given
        int threadCount = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(threadCount * 2);

        // when
        for (int i = 1; i <= threadCount; i++) {
            final long userId = i;
            // 1. 유저 진입 쓰레드
            executorService.submit(() -> {
                try {
                    queueFacade.enterQueue(userId);
                } finally {
                    latch.countDown();
                }
            });

            // 2. 스케줄러 쓰레드 (가상의 스케줄러 동작: makeActive 후 removeWaiting)
            // 실제 QueueScheduler의 로직을 단순화하여 경합 유발
            executorService.submit(() -> {
                try {
                    queueRepository.makeActive(userId, "token-" + userId, 300);
                    queueRepository.removeWaiting(userId);
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executorService.shutdown();

        // then
        // WAITING 상태와 ACTIVE 상태가 중복으로 존재해서는 안 됨
        for (int i = 1; i <= threadCount; i++) {
            final long userId = i;
            boolean isWaiting = queueRepository.getRank(userId).isPresent();
            boolean isActive = queueRepository.getActiveToken(userId).isPresent();
            
            // 둘 다 true인 경우(중복)는 없어야 함
            assertThat(isWaiting && isActive).isFalse();
        }
    }
}
