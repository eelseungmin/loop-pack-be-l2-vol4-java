package com.loopers.application.coupon;

public record CouponIssueRequestEvent(
        String requestId,
        Long userId,
        Long couponId
) {}
