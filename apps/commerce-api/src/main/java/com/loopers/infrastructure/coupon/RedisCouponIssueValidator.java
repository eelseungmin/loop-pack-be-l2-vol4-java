package com.loopers.infrastructure.coupon;

import com.loopers.application.coupon.CouponIssueValidator;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisCouponIssueValidator implements CouponIssueValidator {

    private static final String USER_SET_KEY_PREFIX = "coupon:issue:users:";
    private static final String COUNT_KEY_PREFIX = "coupon:issue:count:";

    private static final long RESULT_DUPLICATE = -1L;
    private static final long RESULT_EXHAUSTED = -2L;

    private final RedisTemplate<String, String> defaultRedisTemplate;

    private static final String LUA_SCRIPT =
            "local is_member = redis.call('SISMEMBER', KEYS[1], ARGV[1])\n" +
            "if is_member == 1 then\n" +
            "    return -1\n" +
            "end\n" +
            "local current_count = tonumber(redis.call('GET', KEYS[2]) or \"0\")\n" +
            "local total_quantity = tonumber(ARGV[2])\n" +
            "if current_count >= total_quantity then\n" +
            "    return -2\n" +
            "end\n" +
            "redis.call('SADD', KEYS[1], ARGV[1])\n" +
            "redis.call('INCR', KEYS[2])\n" +
            "return 1";

    private final RedisScript<Long> script = new DefaultRedisScript<>(LUA_SCRIPT, Long.class);

    @Override
    public void validateIssueRequest(Long userId, Long couponId, Integer totalQuantity) {
        String userSetKey = USER_SET_KEY_PREFIX + couponId;
        String countKey = COUNT_KEY_PREFIX + couponId;

        if (totalQuantity == null || totalQuantity <= 0) {
            throw new CoreException(ErrorType.BAD_REQUEST, "올바르지 않은 총 수량입니다.");
        }

        Long result = defaultRedisTemplate.execute(
                script,
                List.of(userSetKey, countKey),
                String.valueOf(userId),
                String.valueOf(totalQuantity)
        );

        if (result == null) {
            throw new CoreException(ErrorType.INTERNAL_ERROR, "Redis 실행 결과가 null입니다.");
        }

        if (result == RESULT_DUPLICATE) {
            throw new CoreException(ErrorType.CONFLICT, "이미 쿠폰을 발급받은 사용자입니다.");
        } else if (result == RESULT_EXHAUSTED) {
            throw new CoreException(ErrorType.COUPON_EXHAUSTED);
        }
    }
}
