package com.loopers.application.coupon;

public interface CouponIssueValidator {
    void validateIssueRequest(Long userId, Long couponId, Integer totalQuantity);
}
