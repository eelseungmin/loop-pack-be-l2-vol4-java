package com.loopers.domain.coupon;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class CouponTemplateTest {

    @Test
    @DisplayName("쿠폰 템플릿 만료 여부를 검증할 수 있다.")
    void isExpired_ShouldReturnCorrectStatus() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 6, 11, 21, 0);
        CouponTemplate expiredTemplate = new CouponTemplate("만료쿠폰", CouponType.FIXED, new BigDecimal("1000"), null, null, now.minusSeconds(1));
        CouponTemplate activeTemplate = new CouponTemplate("사용가능쿠폰", CouponType.FIXED, new BigDecimal("1000"), null, null, now.plusDays(1));

        // when & then
        assertThat(expiredTemplate.isExpired(now)).isTrue();
        assertThat(activeTemplate.isExpired(now)).isFalse();
    }

    @Test
    @DisplayName("쿠폰 수량이 남아있으면 발급 수량을 1 증가시킨다.")
    void increaseIssuedQuantity_ShouldIncreaseQuantity() {
        // given
        LocalDateTime now = LocalDateTime.now();
        CouponTemplate template = new CouponTemplate("선착순 쿠폰", CouponType.FIXED, new BigDecimal("1000"), null, null, now.plusDays(1), 10, 0);

        // when
        template.increaseIssuedQuantity();

        // then
        assertThat(template.getIssuedQuantity()).isEqualTo(1);
    }

    @Test
    @DisplayName("쿠폰 수량이 소진되었을 때 발급 수량을 증가시키려 하면 예외가 발생한다.")
    void increaseIssuedQuantity_Exhausted_ShouldThrowException() {
        // given
        LocalDateTime now = LocalDateTime.now();
        CouponTemplate template = new CouponTemplate("선착순 쿠폰", CouponType.FIXED, new BigDecimal("1000"), null, null, now.plusDays(1), 10, 10);

        // when & then
        org.junit.jupiter.api.Assertions.assertThrows(com.loopers.support.error.CoreException.class, () -> 
            template.increaseIssuedQuantity()
        );
    }
}
