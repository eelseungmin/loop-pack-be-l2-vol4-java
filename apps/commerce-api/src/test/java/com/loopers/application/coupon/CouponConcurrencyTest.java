package com.loopers.application.coupon;

import com.loopers.domain.coupon.CouponTemplate;
import com.loopers.domain.coupon.CouponType;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@org.springframework.test.context.ContextConfiguration(initializers = com.loopers.testcontainers.RedisTestContainersConfig.class)
class CouponConcurrencyTest {

    @Autowired
    private CouponFacade couponFacade;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("총 수량이 100개인 쿠폰에 대해 200명이 동시에 발급을 요청하면 100명만 성공하고 100명은 실패한다.")
    void issueCoupon_PessimisticLockConcurrency_ShouldLimitToTotalQuantity() throws InterruptedException {
        // given
        CouponTemplate template = couponRepository.saveTemplate(
                new CouponTemplate("선착순 100명 쿠폰", CouponType.FIXED, new BigDecimal("5000"), BigDecimal.ZERO, null, LocalDateTime.now().plusDays(10), 100, 0)
        );
        Long couponTemplateId = template.getId();

        int threadCount = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failCount = new AtomicInteger();

        try {
            // when
            for (int i = 0; i < threadCount; i++) {
                long userId = i + 1;
                executorService.submit(() -> {
                    try {
                        barrier.await();
                        couponFacade.issueCoupon(userId, couponTemplateId);
                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        failCount.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            doneLatch.await();

            // then
            assertThat(successCount.get()).isEqualTo(100);
            assertThat(failCount.get()).isEqualTo(100);

            // DB에 저장된 최종 발급 수량 검증
            CouponTemplate updated = couponRepository.findTemplateById(couponTemplateId).orElseThrow();
            assertThat(updated.getIssuedQuantity()).isEqualTo(100);
        } finally {
            executorService.shutdown();
        }
    }
}
