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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
class PaymentKafkaConsumerTest {

    @InjectMocks
    private PaymentKafkaConsumer paymentKafkaConsumer;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ConsumedMessageRepository consumedMessageRepository;

    @Mock
    private NotificationService notificationService;

    @Mock
    private PaymentCompensationHandler paymentCompensationHandler;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private Acknowledgment acknowledgment;

    @Test
    @DisplayName("결제 완료 메시지가 수신되면, 멱등성을 확인하고 알림을 보낸 뒤 이력을 저장한다.")
    void handlePaymentCompleted_Success() throws Exception {
        // given
        String messageId = "msg-123";
        String payload = "{\"paymentId\":10, \"orderId\":20, \"userId\":30, \"amount\":5000}";
        PaymentCompletedEvent event = new PaymentCompletedEvent(10L, 20L, 30L, new BigDecimal("5000"));

        Mockito.when(consumedMessageRepository.existsById(messageId)).thenReturn(false);
        Mockito.when(objectMapper.readValue(payload, PaymentCompletedEvent.class)).thenReturn(event);

        // when
        paymentKafkaConsumer.handlePaymentCompleted(payload, messageId, acknowledgment);

        // then
        Mockito.verify(notificationService).sendPaymentSuccess(30L, 10L);
        Mockito.verify(consumedMessageRepository).save(any(ConsumedMessage.class));
        Mockito.verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("결제 완료 메시지가 이미 수신된 이력이 있다면, 로직을 수행하지 않고 ack()만 호출한다.")
    void handlePaymentCompleted_AlreadyConsumed_ShouldIgnore() throws Exception {
        // given
        String messageId = "msg-123";
        String payload = "payload";

        Mockito.when(consumedMessageRepository.existsById(messageId)).thenReturn(true);

        // when
        paymentKafkaConsumer.handlePaymentCompleted(payload, messageId, acknowledgment);

        // then
        Mockito.verify(notificationService, Mockito.never()).sendPaymentSuccess(any(), any());
        Mockito.verify(consumedMessageRepository, Mockito.never()).save(any(ConsumedMessage.class));
        Mockito.verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("결제 실패 메시지가 수신되면, 멱등성을 확인하고 보상 로직을 실행한 뒤 이력을 저장한다.")
    void handlePaymentFailed_Success() throws Exception {
        // given
        String messageId = "msg-456";
        String payload = "{\"paymentId\":10, \"orderId\":20, \"userId\":30, \"amount\":5000}";
        PaymentFailedEvent event = new PaymentFailedEvent(10L, 20L, 30L, new BigDecimal("5000"));
        OrderModel order = new OrderModel(30L, null, new BigDecimal("5000"), BigDecimal.ZERO, new BigDecimal("5000"));

        Mockito.when(consumedMessageRepository.existsById(messageId)).thenReturn(false);
        Mockito.when(objectMapper.readValue(payload, PaymentFailedEvent.class)).thenReturn(event);
        Mockito.when(orderRepository.findById(20L)).thenReturn(Optional.of(order));

        // when
        paymentKafkaConsumer.handlePaymentFailed(payload, messageId, acknowledgment);

        // then
        Mockito.verify(paymentCompensationHandler).compensate(order);
        Mockito.verify(consumedMessageRepository).save(any(ConsumedMessage.class));
        Mockito.verify(acknowledgment).acknowledge();
    }
}
