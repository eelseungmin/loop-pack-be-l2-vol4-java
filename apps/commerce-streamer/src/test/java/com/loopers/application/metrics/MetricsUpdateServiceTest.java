package com.loopers.application.metrics;

import com.loopers.domain.metrics.ProductMetrics;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsUpdateServiceTest {

    @Test
    @DisplayName("조회 이벤트를 수신하면 상품 지표의 조회수를 1 증가시킨다.")
    void update_WhenViewEvent_ShouldIncreaseViewCount() {
        // given
        FakeProductMetricsRepository productMetricsRepository = new FakeProductMetricsRepository();
        MetricsUpdateService metricsUpdateService = new MetricsUpdateService(productMetricsRepository);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-1",
            RankingEventType.VIEW,
            1L,
            BigDecimal.ZERO,
            0,
            LocalDateTime.of(2026, 7, 14, 10, 0)
        );

        // when
        metricsUpdateService.update(event);

        // then
        ProductMetrics metrics = productMetricsRepository.saved;
        assertThat(metrics.getProductId()).isEqualTo(1L);
        assertThat(metrics.getTotalViews()).isEqualTo(1L);
        assertThat(metrics.getTotalLikes()).isZero();
        assertThat(metrics.getTotalSales()).isZero();
    }

    @Test
    @DisplayName("주문 이벤트를 수신하면 판매량을 누적한다.")
    void update_WhenOrderEvent_ShouldIncreaseSalesMetrics() {
        // given
        FakeProductMetricsRepository productMetricsRepository = new FakeProductMetricsRepository(
            ProductMetrics.create(1L)
        );
        MetricsUpdateService metricsUpdateService = new MetricsUpdateService(productMetricsRepository);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-2",
            RankingEventType.ORDER,
            1L,
            BigDecimal.valueOf(10_000),
            2,
            LocalDateTime.of(2026, 7, 14, 10, 0)
        );

        // when
        metricsUpdateService.update(event);

        // then
        ProductMetrics metrics = productMetricsRepository.saved;
        assertThat(metrics.getTotalSales()).isEqualTo(2L);
    }

    private static class FakeProductMetricsRepository implements ProductMetricsRepository {
        private ProductMetrics found;
        private ProductMetrics saved;

        private FakeProductMetricsRepository() {
        }

        private FakeProductMetricsRepository(ProductMetrics found) {
            this.found = found;
        }

        @Override
        public Optional<ProductMetrics> findByProductId(Long productId) {
            return Optional.ofNullable(found);
        }

        @Override
        public ProductMetrics save(ProductMetrics productMetrics) {
            this.saved = productMetrics;
            this.found = productMetrics;
            return productMetrics;
        }
    }
}
