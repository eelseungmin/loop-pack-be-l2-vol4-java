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

    private static final String ENTER_SCRIPT =
            "local active_token = redis.call('GET', KEYS[1]) \n" +
            "if active_token then \n" +
            "    return {1, active_token, -1, -1} \n" +
            "end \n" +
            "local current_rank = redis.call('ZRANK', KEYS[2], ARGV[1]) \n" +
            "if not current_rank then \n" +
            "    redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]) \n" +
            "    current_rank = redis.call('ZRANK', KEYS[2], ARGV[1]) \n" +
            "end \n" +
            "local total = redis.call('ZCARD', KEYS[2]) \n" +
            "return {0, '', current_rank, total}";

    @Override
    public QueueEntryResult enterAtomically(Long userId, long score) {
        String activeKey = ACTIVE_KEY_PREFIX + userId;
        org.springframework.data.redis.core.script.DefaultRedisScript<List> script = 
            new org.springframework.data.redis.core.script.DefaultRedisScript<>(ENTER_SCRIPT, List.class);
        
        List<Object> result = defaultRedisTemplate.execute(
            script, 
            List.of(activeKey, WAITING_KEY), 
            String.valueOf(userId), 
            String.valueOf(score)
        );

        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("Redis script returned null");
        }

        long status = (Long) result.get(0);
        if (status == 1L) {
            String token = (String) result.get(1);
            return new QueueEntryResult(true, token, null, null);
        } else {
            Long rank = (Long) result.get(2);
            Long total = (Long) result.get(3);
            return new QueueEntryResult(false, null, rank, total);
        }
    }
}
