package com.loopers.application.queue;

import com.loopers.domain.queue.QueuePosition;
import com.loopers.domain.queue.QueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class QueueFacade {

    private final QueueRepository queueRepository;
    private final QueueService queueService;

    public QueuePosition enterQueue(Long userId) {
        // 1. Check if user is already Active
        Optional<String> activeToken = queueRepository.getActiveToken(userId);
        if (activeToken.isPresent()) {
            return queueService.getQueuePosition(userId, Optional.empty(), activeToken);
        }

        // 2. Check if user is already Waiting (preserves position, prevents duplicates)
        Optional<Long> rank = queueRepository.getRank(userId);
        if (rank.isPresent()) {
            return queueService.getQueuePosition(userId, rank, Optional.empty());
        }

        // 3. New Entry
        queueRepository.enter(userId, System.currentTimeMillis());
        Optional<Long> newRank = queueRepository.getRank(userId);
        return queueService.getQueuePosition(userId, newRank, Optional.empty());
    }

    public QueuePosition getQueuePosition(Long userId) {
        Optional<String> activeToken = queueRepository.getActiveToken(userId);
        Optional<Long> rank = queueRepository.getRank(userId);
        return queueService.getQueuePosition(userId, rank, activeToken);
    }

    public Long getWaitingCount() {
        return queueRepository.getWaitingCount();
    }

    public boolean isValidActiveToken(Long userId, String token) {
        return queueRepository.getActiveToken(userId)
                .map(activeToken -> activeToken.equals(token))
                .orElse(false);
    }
}
