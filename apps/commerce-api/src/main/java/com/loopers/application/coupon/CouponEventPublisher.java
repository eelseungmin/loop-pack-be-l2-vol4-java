package com.loopers.application.coupon;

public interface CouponEventPublisher {
    void publishIssueRequest(String requestId, Long userId, Long couponId);
}
