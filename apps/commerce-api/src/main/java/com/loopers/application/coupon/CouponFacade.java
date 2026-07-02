package com.loopers.application.coupon;

import com.loopers.domain.coupon.CouponIssue;
import com.loopers.application.coupon.CouponRepository;
import com.loopers.domain.coupon.CouponTemplate;
import com.loopers.domain.coupon.UserCouponInfo;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import com.loopers.interfaces.api.coupon.CouponV1Dto.UserCouponResponse;
import com.loopers.domain.coupon.CouponRequestStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class CouponFacade {

    private final CouponRepository couponRepository;
    private final CouponRequestRepository couponRequestRepository;
    private final CouponIssueValidator couponIssueValidator;
    private final CouponEventPublisher couponEventPublisher;

    @Transactional
    public CouponIssue issueCoupon(Long userId, Long couponTemplateId) {
        CouponTemplate template = couponRepository.findTemplateById(couponTemplateId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "조회할 수 없는 쿠폰템플릿입니다."));

        couponRepository.findIssueByUserIdAndTemplateId(userId, couponTemplateId)
                .ifPresent(issue -> {
                    throw new CoreException(ErrorType.CONFLICT, "이미 발급된 쿠폰입니다.");
                });

        CouponIssue newIssue = new CouponIssue(userId, template);
        return couponRepository.saveIssue(newIssue);
    }

    @Transactional(readOnly = true)
    public List<UserCouponResponse> getUsersCoupons(Long userId) {
        List<CouponIssue> issues = couponRepository.findAllIssuesByUserId(userId);
        if (issues.isEmpty()) {
            return List.of();
        }

        LocalDateTime now = LocalDateTime.now();

        return issues.stream()
                .map(issue -> UserCouponInfo.of(issue, now))
                .map(info -> new UserCouponResponse(
                        info.id(),
                        info.couponTemplateId(),
                        info.name(),
                        info.type(),
                        info.value(),
                        info.minOrderAmount(),
                        info.maxDiscountAmount(),
                        info.status(),
                        info.expiredAt()
                ))
                .toList();
    }

    public CouponRequestStatus getRequestStatus(String requestId) {
        return couponRequestRepository.findStatus(requestId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "조회할 수 없는 요청ID입니다."));
    }

    public String issueCouponAsync(Long userId, Long couponTemplateId) {
        CouponTemplate template = couponRepository.findTemplateById(couponTemplateId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "조회할 수 없는 쿠폰템플릿입니다."));

        couponIssueValidator.validateIssueRequest(userId, couponTemplateId, template.getTotalQuantity());

        String requestId = UUID.randomUUID().toString();
        couponRequestRepository.saveStatus(requestId, CouponRequestStatus.IN_PROGRESS);

        couponEventPublisher.publishIssueRequest(requestId, userId, couponTemplateId);

        return requestId;
    }
}
