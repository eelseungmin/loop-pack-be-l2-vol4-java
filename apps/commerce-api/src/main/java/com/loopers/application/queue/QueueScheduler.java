package com.loopers.application.queue;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueueScheduler {

    private final QueueRepository queueRepository;
    private final RedissonClient redissonClient;

    private static final int BATCH_SIZE = 21;
    private static final long ACTIVE_TTL_SECONDS = 300L; // 5 minutes
    private static final String LOCK_KEY = "lock:queue:scheduler";

    @Scheduled(fixedDelay = 1000)
    public void promoteWaitingUsers() {
        RLock lock = redissonClient.getLock(LOCK_KEY);
        try {
            if (lock.tryLock(0, 10, TimeUnit.SECONDS)) {
                try {
                    List<Long> waitingUsers = queueRepository.getFirstNWaiting(BATCH_SIZE);
                    if (!waitingUsers.isEmpty()) {
                        log.info("Promoting {} users from waiting queue", waitingUsers.size());
                        for (Long userId : waitingUsers) {
                            String token = UUID.randomUUID().toString();
                            queueRepository.makeActive(userId, token, ACTIVE_TTL_SECONDS);
                        }
                        queueRepository.removeWaitingUsers(waitingUsers);
                    }
                } finally {
                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                }
            }
        } catch (InterruptedException e) {
            log.error("Queue promotion scheduler interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Error occurred during queue promotion", e);
        }
    }
}
