package com.loopers.domain.queue;

import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class QueueService {

    private static final long BATCH_SIZE = 21L;

    public QueuePosition getQueuePosition(Long userId, Optional<Long> rankOpt, Optional<String> tokenOpt) {
        if (tokenOpt.isPresent()) {
            return QueuePosition.active(userId, tokenOpt.get());
        }
        if (rankOpt.isPresent()) {
            long rank = rankOpt.get();
            long estimatedWaitTime = rank / BATCH_SIZE;
            // rank + 1 to convert 0-based rank to 1-based rank for user visibility
            return QueuePosition.waiting(userId, rank + 1, estimatedWaitTime);
        }
        return QueuePosition.none(userId);
    }
}
