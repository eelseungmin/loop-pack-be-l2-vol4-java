package com.loopers.application.coupon;

import com.loopers.domain.coupon.CouponRequestStatus;
import java.util.Optional;

public interface CouponRequestRepository {
    void saveStatus(String requestId, CouponRequestStatus status);
    Optional<CouponRequestStatus> findStatus(String requestId);
}
