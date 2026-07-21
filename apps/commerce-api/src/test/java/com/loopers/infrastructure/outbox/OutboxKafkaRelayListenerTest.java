package com.loopers.infrastructure.outbox;

import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventCreatedEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ContextConfiguration(initializers = com.loopers.testcontainers.RedisTestContainersConfig.class)
class OutboxKafkaRelayListenerTest {

    @Autowired
    private com.loopers.domain.event.EventPublisher eventPublisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @MockBean
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Test
    @DisplayName("OutboxEventCreatedEvent 발행 시 AFTER_COMMIT 이후 카프카로 전송되고, 성공하면 상태가 COMPLETED로 변경된다.")
    void handleOutboxEventCreated_ShouldSendToKafkaAndComplete() throws InterruptedException {
        // given
        OutboxEvent outboxEvent = new OutboxEvent(EventType.PAYMENT_COMPLETED, "{\"paymentId\":1}", OutboxEventStatus.INIT);
        outboxEventRepository.save(outboxEvent);

        Mockito.doReturn(CompletableFuture.completedFuture(null))
                .when(kafkaTemplate).send(anyString(), anyString(), anyString());

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(new OutboxEventCreatedEvent(outboxEvent.getId()));
        });

        // 비동기 처리(Async) 대기
        Thread.sleep(1000);

        // then
        Mockito.verify(kafkaTemplate, Mockito.times(1))
                .send(eq("PAYMENT_COMPLETED"), eq(outboxEvent.getId().toString()), eq("{\"paymentId\":1}"));

        OutboxEvent updated = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(OutboxEventStatus.COMPLETED);
    }

    @Test
    @DisplayName("PRODUCT_RANKING_EVENT 발행 시 product-events 토픽으로 보내고 payload에 Outbox id를 eventId로 주입한다.")
    void handleOutboxEventCreated_WhenProductRankingEvent_ShouldInjectEventId() throws InterruptedException {
        // given
        String rawPayload = "{\"rankingEventType\":\"PRODUCT_DELETED\",\"productId\":1,\"price\":0,\"amount\":0,\"occurredAt\":\"2026-07-14T10:00:00\"}";
        OutboxEvent outboxEvent = new OutboxEvent(EventType.PRODUCT_RANKING_EVENT, rawPayload, OutboxEventStatus.INIT);
        outboxEventRepository.save(outboxEvent);

        Mockito.doReturn(CompletableFuture.completedFuture(null))
            .when(kafkaTemplate).send(anyString(), anyString(), anyString());

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(new OutboxEventCreatedEvent(outboxEvent.getId()));
        });

        Thread.sleep(1000);

        // then
        String expectedPayload = "{\"rankingEventType\":\"PRODUCT_DELETED\",\"productId\":1,\"price\":0,\"amount\":0,\"occurredAt\":\"2026-07-14T10:00:00\",\"eventId\":\"" + outboxEvent.getId() + "\"}";
        verify(kafkaTemplate).send(
            eq("product-events"),
            eq(outboxEvent.getId().toString()),
            eq(expectedPayload)
        );

        OutboxEvent updated = outboxEventRepository.findById(outboxEvent.getId()).orElseThrow();
        assertThat(updated.getPayload()).isEqualTo(rawPayload);
        assertThat(updated.getStatus()).isEqualTo(OutboxEventStatus.COMPLETED);
    }
}
