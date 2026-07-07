package com.loopers.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.event.EventPublisher;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventCreatedEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.payment.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCompletedOutboxListener {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final EventPublisher eventPublisher;

    @TransactionalEventListener(phase = TransactionPhase.BEFORE_COMMIT)
    public void saveOutboxEvent(PaymentCompletedEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            OutboxEvent outboxEvent = new OutboxEvent(EventType.PAYMENT_COMPLETED, payload, OutboxEventStatus.INIT);
            outboxEventRepository.save(outboxEvent);
            eventPublisher.publish(new OutboxEventCreatedEvent(outboxEvent.getId()));
            log.info("Successfully saved PaymentCompletedEvent to outbox.");
        } catch (Exception e) {
            log.error("Failed to save outbox event for PAYMENT_COMPLETED. paymentId: {}", event.paymentId(), e);
            throw new RuntimeException("Outbox save failed", e);
        }
    }
}
