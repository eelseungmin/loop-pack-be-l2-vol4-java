package com.loopers.infrastructure.coupon;

import com.loopers.application.coupon.CouponEventPublisher;
import com.loopers.application.coupon.CouponIssueRequestEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class KafkaCouponEventPublisher implements CouponEventPublisher {

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    private static final String TOPIC_COUPON_ISSUE = "coupon-issue-request";

    @Override
    public void publishIssueRequest(String requestId, Long userId, Long couponId) {
        CouponIssueRequestEvent event = new CouponIssueRequestEvent(requestId, userId, couponId);
        kafkaTemplate.send(TOPIC_COUPON_ISSUE, String.valueOf(userId), event);
    }
}
