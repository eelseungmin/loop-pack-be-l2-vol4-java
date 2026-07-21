package com.loopers.domain.metrics;

import com.loopers.domain.BaseTimeEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Entity
@Table(name = "product_metrics")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProductMetrics extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "product_id", nullable = false, unique = true)
    private Long productId;

    @Column(name = "total_views", nullable = false)
    private long totalViews;

    @Column(name = "total_likes", nullable = false)
    private long totalLikes;

    @Column(name = "total_sales", nullable = false)
    private long totalSales;

    private ProductMetrics(Long productId) {
        this.productId = productId;
    }

    public static ProductMetrics create(Long productId) {
        return new ProductMetrics(productId);
    }

    public void addView() {
        this.totalViews++;
    }

    public void addLike() {
        this.totalLikes++;
    }

    public void addSales(int amount) {
        this.totalSales += amount;
    }
}
