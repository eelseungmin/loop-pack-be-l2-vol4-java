package com.loopers.infrastructure.queue;

import com.loopers.application.queue.QueueRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Repository
@RequiredArgsConstructor
public class RedisQueueRepository implements QueueRepository {

    private static final String WAITING_KEY = "queue:waiting";
    private static final String ACTIVE_KEY_PREFIX = "queue:active:";

    private final RedisTemplate<String, String> defaultRedisTemplate;

    @Override
    public void enter(Long userId, long score) {
        defaultRedisTemplate.opsForZSet().add(WAITING_KEY, String.valueOf(userId), score);
    }

    @Override
    public Optional<Long> getRank(Long userId) {
        Long rank = defaultRedisTemplate.opsForZSet().rank(WAITING_KEY, String.valueOf(userId));
        return Optional.ofNullable(rank);
    }

    @Override
    public Long getWaitingCount() {
        Long size = defaultRedisTemplate.opsForZSet().size(WAITING_KEY);
        return size != null ? size : 0L;
    }

    @Override
    public void makeActive(Long userId, String token, long ttlSeconds) {
        String key = ACTIVE_KEY_PREFIX + userId;
        defaultRedisTemplate.opsForValue().set(key, token, ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    public Optional<String> getActiveToken(Long userId) {
        String key = ACTIVE_KEY_PREFIX + userId;
        String token = defaultRedisTemplate.opsForValue().get(key);
        return Optional.ofNullable(token);
    }

    @Override
    public void removeWaiting(Long userId) {
        defaultRedisTemplate.opsForZSet().remove(WAITING_KEY, String.valueOf(userId));
    }

    @Override
    public void removeActive(Long userId) {
        String key = ACTIVE_KEY_PREFIX + userId;
        defaultRedisTemplate.delete(key);
    }

    @Override
    public List<Long> getFirstNWaiting(int n) {
        if (n <= 0) {
            return Collections.emptyList();
        }
        Set<String> range = defaultRedisTemplate.opsForZSet().range(WAITING_KEY, 0, n - 1);
        if (range == null || range.isEmpty()) {
            return Collections.emptyList();
        }
        return range.stream().map(Long::valueOf).collect(Collectors.toList());
    }

    @Override
    public void removeWaitingUsers(List<Long> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        Object[] values = userIds.stream().map(String::valueOf).toArray();
        defaultRedisTemplate.opsForZSet().remove(WAITING_KEY, values);
    }
}
