package com.loopers.application.like;

import com.loopers.application.brand.BrandRepository;
import com.loopers.application.product.ProductRepository;
import com.loopers.domain.brand.BrandModel;
import com.loopers.domain.event.EventPublisher;
import com.loopers.domain.like.LikeCreatedEvent;
import com.loopers.domain.like.ProductLikeModel;
import com.loopers.domain.product.ProductModel;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.outbox.EventType;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class LikeEventListenerTest {

    @Autowired
    private LikeFacade likeFacade;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private BrandRepository brandRepository;

    @Autowired
    private LikeRepository likeRepository;

    @Autowired
    private EventPublisher eventPublisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("좋아요 등록 이벤트가 발행되면 비동기적으로 상품의 좋아요 개수가 증가한다.")
    void likeCreatedEvent_ShouldIncreaseProductLikeCount() throws InterruptedException {
        // given
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = productRepository.save(new ProductModel(brand.getId(), "Air Max", new java.math.BigDecimal("1000")));
        Long userId = 1L;

        // when
        likeFacade.addLike(userId, product.getId());

        // 비동기 처리 대기
        Thread.sleep(500);

        // then
        ProductModel updatedProduct = productRepository.findById(product.getId()).orElseThrow();
        assertThat(updatedProduct.getLikeCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("좋아요 취소 이벤트가 발행되면 비동기적으로 상품의 좋아요 개수가 감소한다.")
    void likeDeletedEvent_ShouldDecreaseProductLikeCount() throws InterruptedException {
        // given
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = productRepository.save(new ProductModel(brand.getId(), "Air Max", new java.math.BigDecimal("1000")));
        product.increaseLikeCount(); // 초기 좋아요 수 1로 설정
        productRepository.save(product);

        Long userId = 1L;
        likeRepository.save(new ProductLikeModel(userId, product.getId()));

        // when
        likeFacade.removeLike(userId, product.getId());

        // 비동기 처리 대기
        Thread.sleep(500);

        // then
        ProductModel updatedProduct = productRepository.findById(product.getId()).orElseThrow();
        assertThat(updatedProduct.getLikeCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("좋아요 등록 이벤트 처리 중 예외가 발생하면 Outbox에 INIT 상태로 이벤트를 기록한다.")
    void likeCreatedEvent_Failure_ShouldSaveToOutbox() throws InterruptedException {
        // given
        Long userId = 1L;
        Long nonExistentProductId = 9999L;
        LikeCreatedEvent event = new LikeCreatedEvent(userId, nonExistentProductId);

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(event);
        });

        // 비동기 대기
        Thread.sleep(500);

        // then
        java.util.List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        assertThat(outboxEvents).isNotEmpty();
        OutboxEvent outboxEvent = outboxEvents.stream()
                .filter(e -> e.getEventType() == EventType.LIKE_CREATED)
                .findFirst()
                .orElseThrow();
        assertThat(outboxEvent.getPayload()).contains(String.valueOf(nonExistentProductId));
    }
}
