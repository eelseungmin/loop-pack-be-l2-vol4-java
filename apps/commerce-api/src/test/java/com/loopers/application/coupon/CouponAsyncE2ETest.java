package com.loopers.application.coupon;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.domain.coupon.CouponRequestStatus;
import com.loopers.domain.coupon.CouponTemplate;
import com.loopers.domain.coupon.CouponType;
import com.loopers.infrastructure.consumer.CouponKafkaConsumer;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class CouponAsyncE2ETest {

    @Autowired
    private CouponFacade couponFacade;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private CouponRequestRepository couponRequestRepository;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @Autowired
    private org.springframework.data.redis.core.RedisTemplate<String, String> defaultRedisTemplate;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // 카프카 없이 비동기 처리를 흉내 내기 위한 테스트 전용 설정을 주입한다.
    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        public CouponEventPublisher fakeCouponEventPublisher(org.springframework.context.ApplicationContext context, ObjectMapper objectMapper) {
            ExecutorService executor = Executors.newFixedThreadPool(10);
            return new CouponEventPublisher() {
                private CouponKafkaConsumer consumer;

                @Override
                public void publishIssueRequest(String requestId, Long userId, Long couponId) {
                    if (consumer == null) {
                        consumer = context.getBean(CouponKafkaConsumer.class);
                    }
                    executor.submit(() -> {
                        try {
                            CouponIssueRequestEvent event = new CouponIssueRequestEvent(requestId, userId, couponId);
                            String payload = objectMapper.writeValueAsString(event);
                            Acknowledgment acknowledgment = mock(Acknowledgment.class);
                            consumer.handleCouponIssueRequest(payload, requestId, acknowledgment);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            };
        }
    }

    @Test
    @DisplayName("10개 한정 선착순 쿠폰에 대해 50명이 동시에 비동기 발급 요청을 시도하면 정확히 10명만 성공하고 40명은 실패한다.")
    void asyncIssueCoupon_ShouldLimitToTotalQuantity() throws InterruptedException, ExecutionException {
        // given
        CouponTemplate template = couponRepository.saveTemplate(
                new CouponTemplate("선착순 10명 쿠폰", CouponType.FIXED, new BigDecimal("5000"), BigDecimal.ZERO, null, LocalDateTime.now().plusDays(10), 10, 0)
        );
        Long couponTemplateId = template.getId();

        int threadCount = 50;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        List<String> requestIds = new CopyOnWriteArrayList<>();
        AtomicInteger requestSuccessCount = new AtomicInteger();
        AtomicInteger requestFailCount = new AtomicInteger();

        // when
        for (int i = 0; i < threadCount; i++) {
            long userId = i + 1;
            executorService.submit(() -> {
                try {
                    barrier.await();
                    String requestId = couponFacade.issueCouponAsync(userId, couponTemplateId);
                    requestIds.add(requestId);
                    requestSuccessCount.incrementAndGet();
                } catch (Exception e) {
                    requestFailCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        doneLatch.await();
        executorService.shutdown();

        // then: 1차 검증 (Redis 1차 고속 필터링)
        assertThat(requestSuccessCount.get()).isEqualTo(10);
        assertThat(requestFailCount.get()).isEqualTo(40);
        assertThat(requestIds).hasSize(10);

        // 비동기 처리 대기 폴링 (최대 5초)
        boolean allProcessed = false;
        for (int i = 0; i < 50; i++) {
            int successStatusCount = 0;
            for (String rid : requestIds) {
                CouponRequestStatus status = couponRequestRepository.findStatus(rid).orElse(null);
                if (status == CouponRequestStatus.SUCCESS) {
                    successStatusCount++;
                }
            }
            if (successStatusCount == 10) {
                allProcessed = true;
                break;
            }
            Thread.sleep(100);
        }

        assertThat(allProcessed).isTrue();

        // 최종 E2E 정합성 검증
        // 1. DB의 issued_quantity 가 10이어야 함
        CouponTemplate updated = couponRepository.findTemplateById(couponTemplateId).orElseThrow();
        assertThat(updated.getIssuedQuantity()).isEqualTo(10);

        // 2. DB에 실제 저장된 CouponIssue 발급 건수가 10건이어야 함
        long issueCount = couponRepository.findAllIssuesByTemplateId(couponTemplateId, org.springframework.data.domain.Pageable.unpaged()).getTotalElements();
        assertThat(issueCount).isEqualTo(10);
    }
}
