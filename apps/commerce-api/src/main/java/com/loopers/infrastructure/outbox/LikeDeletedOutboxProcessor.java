package com.loopers.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.product.ProductRepository;
import com.loopers.domain.like.LikeDeletedEvent;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.product.ProductModel;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
public class LikeDeletedOutboxProcessor implements OutboxProcessor {

    private final ProductRepository productRepository;
    private final ObjectMapper objectMapper;

    @Override
    public boolean supports(EventType eventType) {
        return eventType == EventType.LIKE_DELETED;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void process(String payload) throws Exception {
        LikeDeletedEvent event = objectMapper.readValue(payload, LikeDeletedEvent.class);
        ProductModel product = productRepository.findByIdWithLock(event.productId())
                .orElseThrow(() -> new CoreException(ErrorType.PRODUCT_NOT_FOUND));
        product.decreaseLikeCount();
        productRepository.save(product);
    }
}
