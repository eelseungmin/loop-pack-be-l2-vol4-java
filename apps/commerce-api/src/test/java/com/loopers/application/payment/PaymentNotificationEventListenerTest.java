package com.loopers.application.payment;

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
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class PaymentNotificationEventListenerTest {

    @Autowired
    private EventPublisher eventPublisher;

    @SpyBean
    private NotificationService notificationService;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("결제 완료 이벤트를 받으면 유저에게 알림톡을 성공적으로 발송한다.")
    void handlePaymentCompleted_Success() throws InterruptedException {
        // given
        PaymentCompletedEvent event = new PaymentCompletedEvent(1L, 2L, 3L, new BigDecimal("1000"));

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(event);
        });

        Thread.sleep(500);

        // then
        verify(notificationService).sendPaymentSuccess(3L, 1L);
    }

    @Test
    @DisplayName("알림톡 발송 실패 시 Outbox 테이블에 PAYMENT_COMPLETED 타입의 INIT 상태 이벤트가 저장된다.")
    void handlePaymentCompleted_Failure_ShouldSaveToOutbox() throws InterruptedException {
        // given
        PaymentCompletedEvent event = new PaymentCompletedEvent(1L, 2L, 3L, new BigDecimal("1000"));
        doThrow(new RuntimeException("Notification service timeout"))
                .when(notificationService).sendPaymentSuccess(Mockito.anyLong(), Mockito.anyLong());

        // when
        transactionTemplate.executeWithoutResult(status -> {
            eventPublisher.publish(event);
        });

        Thread.sleep(500);

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
