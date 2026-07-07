package com.loopers.infrastructure.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.inbox.ConsumedMessageRepository;
import com.loopers.application.order.OrderRepository;
import com.loopers.application.payment.NotificationService;
import com.loopers.application.payment.PaymentCompensationHandler;
import com.loopers.domain.inbox.ConsumedMessage;
import com.loopers.domain.order.OrderModel;
import com.loopers.domain.payment.PaymentCompletedEvent;
import com.loopers.domain.payment.PaymentFailedEvent;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final ConsumedMessageRepository consumedMessageRepository;
    private final NotificationService notificationService;
    private final PaymentCompensationHandler paymentCompensationHandler;
    private final OrderRepository orderRepository;

    @Transactional
    @KafkaListener(topics = "PAYMENT_COMPLETED", groupId = "commerce-api-payment-group")
    public void handlePaymentCompleted(
            String payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String messageId,
            Acknowledgment acknowledgment) {
            
        try {
            if (consumedMessageRepository.existsById(messageId)) {
                log.info("Already consumed message. topic: PAYMENT_COMPLETED, messageId: {}", messageId);
                acknowledgment.acknowledge();
                return;
            }

            PaymentCompletedEvent event = parsePayload(payload, PaymentCompletedEvent.class);
            notificationService.sendPaymentSuccess(event.userId(), event.paymentId());

            consumedMessageRepository.save(new ConsumedMessage(messageId, "PAYMENT_COMPLETED"));
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process PAYMENT_COMPLETED message. messageId: {}", messageId, e);
            throw new RuntimeException(e);
        }
    }

    @Transactional
    @KafkaListener(topics = "PAYMENT_FAILED", groupId = "commerce-api-payment-group")
    public void handlePaymentFailed(
            String payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String messageId,
            Acknowledgment acknowledgment) {
            
        try {
            if (consumedMessageRepository.existsById(messageId)) {
                log.info("Already consumed message. topic: PAYMENT_FAILED, messageId: {}", messageId);
                acknowledgment.acknowledge();
                return;
            }

            PaymentFailedEvent event = parsePayload(payload, PaymentFailedEvent.class);
            
            OrderModel order = orderRepository.findById(event.orderId())
                    .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "주문 내역을 찾을 수 없습니다."));

            paymentCompensationHandler.compensate(order);

            consumedMessageRepository.save(new ConsumedMessage(messageId, "PAYMENT_FAILED"));
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process PAYMENT_FAILED message. messageId: {}", messageId, e);
            throw new RuntimeException(e);
        }
    }

    private <T> T parsePayload(String payload, Class<T> clazz) throws Exception {
        // payload가 이중 직렬화된 문자열일 경우를 대비하여 처리
        if (payload.startsWith("\"") && payload.endsWith("\"")) {
            String unescaped = objectMapper.readValue(payload, String.class);
            return objectMapper.readValue(unescaped, clazz);
        }
        return objectMapper.readValue(payload, clazz);
    }
}
