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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.kafka.core.KafkaTemplate;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
@TestPropertySource(properties = "scheduling.enabled=true")
class OutboxSchedulerTest {

    @Autowired
    private OutboxScheduler outboxScheduler;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockBean
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("Outbox 테이블의 INIT 상태 이벤트들을 스케줄러가 카프카로 발송하고 완료 상태로 업데이트한다.")
    void run_ShouldSendInitEventsToKafka() throws Exception {
        // given
        PaymentCompletedEvent paymentEvent = new PaymentCompletedEvent(10L, 20L, 30L, new BigDecimal("5000"));
        String paymentPayload = objectMapper.writeValueAsString(paymentEvent);
        OutboxEvent paymentOutbox = outboxEventRepository.save(new OutboxEvent(EventType.PAYMENT_COMPLETED, paymentPayload, OutboxEventStatus.INIT));

        Mockito.doReturn(CompletableFuture.completedFuture(null))
                .when(kafkaTemplate).send(anyString(), anyString(), anyString());

        // when
        outboxScheduler.run();

        // then: 카프카로 전송됨
        Mockito.verify(kafkaTemplate, Mockito.times(1))
                .send(eq("PAYMENT_COMPLETED"), eq(paymentOutbox.getId().toString()), eq(paymentPayload));

        // then: Outbox 상태 COMPLETED로 변경 검증
        OutboxEvent processedPayment = outboxEventRepository.findById(paymentOutbox.getId()).orElseThrow();
        assertThat(processedPayment.getStatus()).isEqualTo(OutboxEventStatus.COMPLETED);
    }
}
