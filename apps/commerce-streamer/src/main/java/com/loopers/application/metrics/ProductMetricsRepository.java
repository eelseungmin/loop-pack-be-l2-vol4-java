package com.loopers.application.metrics;

import com.loopers.domain.metrics.ProductMetrics;

import java.util.Optional;

public interface ProductMetricsRepository {
    Optional<ProductMetrics> findByProductId(Long productId);

    ProductMetrics save(ProductMetrics productMetrics);
}
