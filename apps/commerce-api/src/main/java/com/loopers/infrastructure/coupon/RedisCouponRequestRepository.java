package com.loopers.infrastructure.coupon;

import com.loopers.application.coupon.CouponRequestRepository;
import com.loopers.domain.coupon.CouponRequestStatus;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class RedisCouponRequestRepository implements CouponRequestRepository {

    private static final String KEY_PREFIX = "coupon:request:status:";
    private static final Duration TTL = Duration.ofMinutes(10);

    private final RedisTemplate<String, String> defaultRedisTemplate;

    @Override
    public void saveStatus(String requestId, CouponRequestStatus status) {
        String key = KEY_PREFIX + requestId;
        defaultRedisTemplate.opsForValue().set(key, status.name(), TTL);
    }

    @Override
    public Optional<CouponRequestStatus> findStatus(String requestId) {
        String key = KEY_PREFIX + requestId;
        String value = defaultRedisTemplate.opsForValue().get(key);
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(CouponRequestStatus.valueOf(value));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
