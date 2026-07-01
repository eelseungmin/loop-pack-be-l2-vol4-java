package com.loopers.application.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.payment.PaymentCompletedEvent;
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
public class PaymentNotificationEventListener {

    private final NotificationService notificationService;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Handling PaymentCompletedEvent: paymentId={}, userId={}", event.paymentId(), event.userId());
        try {
            notificationService.sendPaymentSuccess(event.userId(), event.paymentId());
            log.info("Successfully sent payment completed notification to user {}", event.userId());
        } catch (Exception e) {
            log.error("Failed to send payment completed notification for payment {}. Saving to outbox.", event.paymentId(), e);
            saveToOutbox(event, e.getMessage());
        }
    }

    private void saveToOutbox(PaymentCompletedEvent event, String errorMessage) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    String payload = objectMapper.writeValueAsString(event);
                    OutboxEvent outboxEvent = new OutboxEvent(EventType.PAYMENT_COMPLETED, payload, OutboxEventStatus.INIT);
                    outboxEvent.recordError(errorMessage);
                    outboxEventRepository.save(outboxEvent);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            log.info("Successfully saved PaymentCompletedEvent to outbox.");
        } catch (Exception ex) {
            log.error("Failed to save PaymentCompletedEvent to outbox.", ex);
        }
    }
}
