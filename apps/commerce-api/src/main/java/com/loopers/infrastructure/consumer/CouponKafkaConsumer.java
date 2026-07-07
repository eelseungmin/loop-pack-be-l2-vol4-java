package com.loopers.infrastructure.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.coupon.CouponFacade;
import com.loopers.application.coupon.CouponRequestRepository;
import com.loopers.application.coupon.CouponIssueRequestEvent;
import com.loopers.domain.coupon.CouponRequestStatus;
import com.loopers.support.error.CoreException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CouponKafkaConsumer {

    private final ObjectMapper objectMapper;
    private final CouponFacade couponFacade;
    private final CouponRequestRepository couponRequestRepository;

    @KafkaListener(topics = "coupon-issue-request", groupId = "commerce-api-coupon-group")
    public void handleCouponIssueRequest(
            String payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String messageId,
            Acknowledgment acknowledgment) {

        log.info("Received coupon issue request. messageId (Kafka Key): {}", messageId);

        String requestId = "unknown";
        try {
            CouponIssueRequestEvent event = parsePayload(payload, CouponIssueRequestEvent.class);
            requestId = event.requestId();

            CouponRequestStatus status = couponRequestRepository.findStatus(requestId).orElse(null);

            if (status == null) {
                log.warn("No status found for requestId: {}. Ignore message.", requestId);
                acknowledgment.acknowledge();
                return;
            }

            if (status == CouponRequestStatus.SUCCESS || status == CouponRequestStatus.FAILED) {
                log.info("Message already processed for requestId: {}. Status: {}. Acknowledge and ignore.", requestId, status);
                acknowledgment.acknowledge();
                return;
            }

            couponFacade.issueCoupon(event.userId(), event.couponId());

            couponRequestRepository.saveStatus(requestId, CouponRequestStatus.SUCCESS);
            acknowledgment.acknowledge();
            log.info("Successfully processed coupon issue request. requestId: {}", requestId);

        } catch (CoreException e) {
            log.warn("Business exception during processing coupon issue. requestId: {}, error: {}", requestId, e.getMessage());
            couponRequestRepository.saveStatus(requestId, CouponRequestStatus.FAILED);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Infrastructure or unknown exception during processing coupon issue. requestId: {}", requestId, e);
            throw new RuntimeException(e);
        }
    }

    private <T> T parsePayload(String payload, Class<T> clazz) throws Exception {
        if (payload.startsWith("\"") && payload.endsWith("\"")) {
            String unescaped = objectMapper.readValue(payload, String.class);
            return objectMapper.readValue(unescaped, clazz);
        }
        return objectMapper.readValue(payload, clazz);
    }
}
