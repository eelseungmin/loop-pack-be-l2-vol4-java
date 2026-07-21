package com.loopers.application.metrics;

import com.loopers.domain.metrics.ProductMetrics;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MetricsUpdateService {

    private final ProductMetricsRepository productMetricsRepository;

    public void update(ProductRankingEvent event) {
        if (event.eventType() == RankingEventType.PRODUCT_DELETED) {
            return;
        }

        ProductMetrics productMetrics = productMetricsRepository.findByProductId(event.productId())
            .orElseGet(() -> ProductMetrics.create(event.productId()));

        if (event.eventType() == RankingEventType.VIEW) {
            productMetrics.addView();
        }
        if (event.eventType() == RankingEventType.LIKE) {
            productMetrics.addLike();
        }
        if (event.eventType() == RankingEventType.ORDER) {
            productMetrics.addSales(event.amount());
        }

        productMetricsRepository.save(productMetrics);
    }
}
