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
        QueueRepository.QueueEntryResult result = queueRepository.enterAtomically(userId, System.currentTimeMillis());

        if (result.isActive()) {
            return queueService.getQueuePosition(userId, Optional.empty(), Optional.of(result.activeToken()));
        }

        return queueService.getQueuePosition(userId, Optional.of(result.rank()), Optional.empty());
    }

    public QueuePosition getQueuePosition(Long userId) {
        // We can use the same atomic script for getQueuePosition, but without ZADD.
        // Wait, getQueuePosition can just use the atomic enter with score 0? No, that would add if not exists.
        // Let's leave getQueuePosition as is, since the sequence diagram for getQueuePosition doesn't use ZADD.
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
