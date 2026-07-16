package com.loopers.domain.ranking;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class RankingScorePolicy {

    private static final double VIEW_WEIGHT = 0.1;
    private static final double LIKE_WEIGHT = 0.2;
    private static final double ORDER_WEIGHT = 0.6;

    public double calculateScore(RankingEventType eventType, BigDecimal price, int amount) {
        return switch (eventType) {
            case VIEW -> VIEW_WEIGHT;
            case LIKE -> LIKE_WEIGHT;
            case ORDER -> ORDER_WEIGHT * Math.log(price.multiply(BigDecimal.valueOf(amount)).doubleValue() + 1);
            case PRODUCT_DELETED -> throw new IllegalArgumentException("Product delete event does not have ranking score.");
        };
    }
}
