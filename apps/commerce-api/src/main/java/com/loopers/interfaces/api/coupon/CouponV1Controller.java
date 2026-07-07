package com.loopers.interfaces.api.coupon;

import com.loopers.application.coupon.CouponFacade;
import com.loopers.interfaces.api.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class CouponV1Controller {

    private final CouponFacade couponFacade;

    @PostMapping("/coupons/{couponId}/issue")
    @org.springframework.web.bind.annotation.ResponseStatus(org.springframework.http.HttpStatus.ACCEPTED)
    public ApiResponse<CouponV1Dto.CouponIssueResponse> issueCoupon(
            @RequestHeader("X-Loopers-UserId") Long userId,
            @PathVariable("couponId") Long couponId
    ) {
        String requestId = couponFacade.issueCouponAsync(userId, couponId);
        return ApiResponse.success(new CouponV1Dto.CouponIssueResponse(requestId));
    }

    @GetMapping("/users/me/coupons")
    public ApiResponse<List<CouponV1Dto.UserCouponResponse>> getUsersCoupons(
            @RequestHeader("X-Loopers-UserId") Long userId
    ) {
        List<CouponV1Dto.UserCouponResponse> responses = couponFacade.getUsersCoupons(userId);
        return ApiResponse.success(responses);
    }

    @GetMapping("/coupons/requests/{requestId}")
    public ApiResponse<CouponV1Dto.CouponRequestStatusResponse> getCouponRequestStatus(
            @PathVariable("requestId") String requestId
    ) {
        com.loopers.domain.coupon.CouponRequestStatus status = couponFacade.getRequestStatus(requestId);
        return ApiResponse.success(new CouponV1Dto.CouponRequestStatusResponse(requestId, status));
    }
}
