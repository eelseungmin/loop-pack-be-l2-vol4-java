package com.loopers.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.brand.BrandRepository;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.application.payment.NotificationService;
import com.loopers.application.product.ProductRepository;
import com.loopers.domain.brand.BrandModel;
import com.loopers.domain.like.LikeCreatedEvent;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.payment.PaymentCompletedEvent;
import com.loopers.domain.product.ProductModel;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class OutboxSchedulerTest {

    @Autowired
    private OutboxScheduler outboxScheduler;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private BrandRepository brandRepository;

    @SpyBean
    private NotificationService notificationService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("Outbox 테이블의 INIT 상태 이벤트들을 스케줄러가 재처리하여 완료 상태로 업데이트하고 비즈니스 로직을 보정한다.")
    void run_ShouldProcessInitEvents() throws Exception {
        // given: 1. 좋아요 집계 실패 이벤트 준비
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = productRepository.save(new ProductModel(brand.getId(), "Air Max", new BigDecimal("1000")));
        
        LikeCreatedEvent likeEvent = new LikeCreatedEvent(1L, product.getId());
        String likePayload = objectMapper.writeValueAsString(likeEvent);
        OutboxEvent likeOutbox = outboxEventRepository.save(new OutboxEvent(EventType.LIKE_CREATED, likePayload, OutboxEventStatus.INIT));

        // given: 2. 알림톡 실패 이벤트 준비
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(10L, 20L, 30L, new BigDecimal("5000"));
        String paymentPayload = objectMapper.writeValueAsString(paymentEvent);
        OutboxEvent paymentOutbox = outboxEventRepository.save(new OutboxEvent(EventType.PAYMENT_COMPLETED, paymentPayload, OutboxEventStatus.INIT));

        // when
        outboxScheduler.run();

        // then: 1. 좋아요 집계 보정 결과 검증 (likeCount 증가)
        ProductModel updatedProduct = productRepository.findById(product.getId()).orElseThrow();
        assertThat(updatedProduct.getLikeCount()).isEqualTo(1);

        // then: 2. 알림톡 재발송 검증
        verify(notificationService).sendPaymentSuccess(30L, 10L);

        // then: 3. Outbox 상태 COMPLETED로 변경 검증
        OutboxEvent processedLike = outboxEventRepository.findById(likeOutbox.getId()).orElseThrow();
        OutboxEvent processedPayment = outboxEventRepository.findById(paymentOutbox.getId()).orElseThrow();
        assertThat(processedLike.getStatus()).isEqualTo(OutboxEventStatus.COMPLETED);
        assertThat(processedPayment.getStatus()).isEqualTo(OutboxEventStatus.COMPLETED);
    }
}
