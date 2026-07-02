package com.loopers.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.event.EventPublisher;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.payment.PaymentCompletedEvent;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class PaymentCompletedOutboxListenerTest {

    @Autowired
    private EventPublisher eventPublisher;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;

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
    @DisplayName("결제 완료 이벤트 발생 시 동일 트랜잭션 내에 OutboxEvent가 INIT 상태로 저장된다.")
    void saveOutboxEvent_ShouldSaveInitStatusInSameTransaction() {
        // given
        PaymentCompletedEvent event = new PaymentCompletedEvent(1L, 2L, 3L, new BigDecimal("1000"));

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(event);
        });

        // then
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        assertThat(outboxEvents).isNotEmpty();
        OutboxEvent outboxEvent = outboxEvents.stream()
                .filter(e -> e.getEventType() == EventType.PAYMENT_COMPLETED)
                .findFirst()
                .orElseThrow();
        assertThat(outboxEvent.getPayload()).contains("3"); // userId
        assertThat(outboxEvent.getPayload()).contains("1"); // paymentId
    }
}
