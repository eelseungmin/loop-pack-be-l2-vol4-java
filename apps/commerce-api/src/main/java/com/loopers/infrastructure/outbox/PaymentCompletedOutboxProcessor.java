package com.loopers.infrastructure.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.payment.NotificationService;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.payment.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentCompletedOutboxProcessor implements OutboxProcessor {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    @Override
    public boolean supports(EventType eventType) {
        return eventType == EventType.PAYMENT_COMPLETED;
    }

    @Override
    public void process(String payload) throws Exception {
        PaymentCompletedEvent event = objectMapper.readValue(payload, PaymentCompletedEvent.class);
        notificationService.sendPaymentSuccess(event.userId(), event.paymentId());
    }
}
