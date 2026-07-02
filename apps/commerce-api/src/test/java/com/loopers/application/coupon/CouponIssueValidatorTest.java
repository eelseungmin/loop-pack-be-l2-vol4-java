package com.loopers.application.coupon;

import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class CouponIssueValidatorTest {

    @Autowired
    private CouponIssueValidator couponIssueValidator;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @Test
    @DisplayName("동일한 유저가 동일 쿠폰을 중복해서 발급 요청하면 CONFLICT 예외가 발생한다.")
    void validateIssueRequest_DuplicateUser_ShouldThrowConflict() {
        // given
        Long userId = 1L;
        Long couponId = 10L;
        Integer totalQuantity = 100;

        // 1차 요청은 성공해야 함
        couponIssueValidator.validateIssueRequest(userId, couponId, totalQuantity);

        // when & then
        CoreException exception = assertThrows(CoreException.class, () ->
            couponIssueValidator.validateIssueRequest(userId, couponId, totalQuantity)
        );
        assertThat(exception.getErrorType()).isEqualTo(ErrorType.CONFLICT);
    }

    @Test
    @DisplayName("발급 수량이 총 수량을 초과하면 COUPON_EXHAUSTED 예외가 발생한다.")
    void validateIssueRequest_ExceededQuantity_ShouldThrowCouponExhausted() {
        // given
        Long couponId = 11L;
        Integer totalQuantity = 2;

        // 1, 2번째 요청은 각기 다른 유저이므로 성공해야 함
        couponIssueValidator.validateIssueRequest(1L, couponId, totalQuantity);
        couponIssueValidator.validateIssueRequest(2L, couponId, totalQuantity);

        // when & then
        CoreException exception = assertThrows(CoreException.class, () ->
            couponIssueValidator.validateIssueRequest(3L, couponId, totalQuantity)
        );
        assertThat(exception.getErrorType()).isEqualTo(ErrorType.COUPON_EXHAUSTED);
    }
}
