package com.loopers.application.like;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.application.product.ProductRepository;
import com.loopers.domain.like.LikeCreatedEvent;
import com.loopers.domain.like.LikeDeletedEvent;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class LikeEventListener {

    private final ProductRepository productRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleLikeCreated(LikeCreatedEvent event) {
        log.info("Handling LikeCreatedEvent for product: {}", event.productId());
        try {
            transactionTemplate.executeWithoutResult(status -> {
                ProductModel product = productRepository.findByIdWithLock(event.productId())
                        .orElseThrow(() -> new CoreException(ErrorType.PRODUCT_NOT_FOUND));
                product.increaseLikeCount();
                productRepository.save(product);
            });
        } catch (Exception e) {
            log.error("Failed to process LikeCreatedEvent for product: {}. Saving to outbox.", event.productId(), e);
            saveToOutbox(EventType.LIKE_CREATED, event, e.getMessage());
        }
    }

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleLikeDeleted(LikeDeletedEvent event) {
        log.info("Handling LikeDeletedEvent for product: {}", event.productId());
        try {
            transactionTemplate.executeWithoutResult(status -> {
                ProductModel product = productRepository.findByIdWithLock(event.productId())
                        .orElseThrow(() -> new CoreException(ErrorType.PRODUCT_NOT_FOUND));
                product.decreaseLikeCount();
                productRepository.save(product);
            });
        } catch (Exception e) {
            log.error("Failed to process LikeDeletedEvent for product: {}. Saving to outbox.", event.productId(), e);
            saveToOutbox(EventType.LIKE_DELETED, event, e.getMessage());
        }
    }

    private void saveToOutbox(EventType eventType, Object event, String errorMessage) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    String payload = objectMapper.writeValueAsString(event);
                    OutboxEvent outboxEvent = new OutboxEvent(eventType, payload, OutboxEventStatus.INIT);
                    outboxEvent.recordError(errorMessage);
                    outboxEventRepository.save(outboxEvent);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        } catch (Exception ex) {
            log.error("Failed to save OutboxEvent for type: {}", eventType, ex);
        }
    }
}
