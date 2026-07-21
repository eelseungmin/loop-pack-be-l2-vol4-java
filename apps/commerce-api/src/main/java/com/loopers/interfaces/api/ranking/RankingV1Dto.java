package com.loopers.interfaces.api.ranking;

import com.loopers.application.ranking.RankingProductInfo;

import java.math.BigDecimal;

public class RankingV1Dto {

    public record RankingResponse(
        long rank,
        double score,
        Long productId,
        String productName,
        Long brandId,
        String brandName,
        BigDecimal price
    ) {
        public static RankingResponse from(RankingProductInfo info) {
            return new RankingResponse(
                info.rank(),
                info.score(),
                info.productId(),
                info.productName(),
                info.brandId(),
                info.brandName(),
                info.price()
            );
        }
    }
}
