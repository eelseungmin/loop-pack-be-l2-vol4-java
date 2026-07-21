package com.loopers.application.product;

import com.loopers.application.ranking.ProductRankingInfo;
import com.loopers.domain.product.ProductModel;

import java.math.BigDecimal;
import java.time.ZonedDateTime;

public record ProductInfo(
    Long id,
    Long brandId,
    String brandName,
    String name,
    BigDecimal price,
    int likeCount,
    ZonedDateTime createdAt,
    ProductRankingInfo ranking
) {
    public ProductInfo(
        Long id,
        Long brandId,
        String brandName,
        String name,
        BigDecimal price,
        int likeCount,
        ZonedDateTime createdAt
    ) {
        this(id, brandId, brandName, name, price, likeCount, createdAt, null);
    }

    public static ProductInfo from(ProductModel product, int likeCount) {
        return new ProductInfo(
            product.getId(),
            product.getBrandId(),
            null,
            product.getName(),
            product.getPrice(),
            likeCount,
            product.getCreatedAt(),
            null
        );
    }

    public static ProductInfo from(ProductModel product, String brandName, int likeCount) {
        return new ProductInfo(
            product.getId(),
            product.getBrandId(),
            brandName,
            product.getName(),
            product.getPrice(),
            likeCount,
            product.getCreatedAt(),
            null
        );
    }

    public ProductInfo withRanking(ProductRankingInfo ranking) {
        return new ProductInfo(id, brandId, brandName, name, price, likeCount, createdAt, ranking);
    }
}
