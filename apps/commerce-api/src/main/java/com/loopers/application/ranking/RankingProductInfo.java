package com.loopers.application.ranking;

import java.math.BigDecimal;

public record RankingProductInfo(
    long rank,
    double score,
    Long productId,
    String productName,
    Long brandId,
    String brandName,
    BigDecimal price
) {
}
