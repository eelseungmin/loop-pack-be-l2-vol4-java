package com.loopers.application.product;

import com.loopers.application.brand.BrandRepository;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.application.product.ProductRepository;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductAdminFacade {

    private final ProductRepository productRepository;
    private final BrandRepository brandRepository;
    private final RedisTemplate<String, String> defaultRedisTemplate;
    private final OutboxEventRepository outboxEventRepository;

    @Transactional
    public Long registerProduct(Long brandId, String name, BigDecimal price, int initialStock) {
        brandRepository.findById(brandId)
                .orElseThrow(() -> new CoreException(ErrorType.BRAND_NOT_FOUND));

        ProductModel product = new ProductModel(brandId, name, price);
        product.assignStock(initialStock);
        
        return productRepository.save(product).getId();
    }

    @Transactional
    public void updateProduct(Long id, String name, BigDecimal price) {
        ProductModel product = productRepository.findById(id)
                .orElseThrow(() -> new CoreException(ErrorType.PRODUCT_NOT_FOUND));
        product.update(name, price);
        productRepository.save(product);
        evictCache(id);
    }

    @Transactional
    public void deleteProduct(Long id) {
        ProductModel product = productRepository.findById(id)
                .orElseThrow(() -> new CoreException(ErrorType.PRODUCT_NOT_FOUND));
        product.delete();
        productRepository.save(product);
        outboxEventRepository.save(new OutboxEvent(EventType.PRODUCT_RANKING_EVENT, productDeletedPayload(id), OutboxEventStatus.INIT));
        evictCache(id);
    }

    private String productDeletedPayload(Long productId) {
        return "{\"rankingEventType\":\"PRODUCT_DELETED\",\"productId\":" + productId + ",\"price\":0,\"amount\":0,\"occurredAt\":\"" + LocalDateTime.now() + "\"}";
    }

    private void evictCache(Long id) {
        String cacheKey = "product:detail::" + id;
        try {
            defaultRedisTemplate.delete(cacheKey);
        } catch (Exception e) {
            log.error("Redis evict error for key: {}", cacheKey, e);
        }
    }
}
