package com.loopers.infrastructure.payment;

import com.loopers.application.payment.PaymentTempStorage;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class RedisPaymentTempStorage implements PaymentTempStorage {

    private final RedisTemplate<String, String> defaultRedisTemplate;
    private final RedissonClient redissonClient;

    @Override
    public void setRetryCount(Long paymentId, int count, Duration ttl) {
        String key = "payment_retry:" + paymentId;
        defaultRedisTemplate.opsForValue().set(key, String.valueOf(count), ttl);
    }

    @Override
    public void deleteRetryKey(Long paymentId) {
        String key = "payment_retry:" + paymentId;
        defaultRedisTemplate.delete(key);
    }

    @Override
    public Integer getRetryCount(Long paymentId) {
        String key = "payment_retry:" + paymentId;
        String val = defaultRedisTemplate.opsForValue().get(key);
        return val != null ? Integer.parseInt(val) : null;
    }

    @Override
    public boolean lockOrder(Long orderId) {
        String key = "payment:lock:" + orderId;
        RLock lock = redissonClient.getLock(key);
        try {
            return lock.tryLock(0, 10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void unlockOrder(Long orderId) {
        String key = "payment:lock:" + orderId;
        RLock lock = redissonClient.getLock(key);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }
}
