package com.loopers.domain.queue;

public record QueuePosition(
        Long userId,
        QueueStatus status,
        Long rank,
        Long estimatedWaitTime,
        Long pollingInterval,
        String token
) {
    public static QueuePosition waiting(Long userId, Long rank, Long estimatedWaitTime) {
        Long pollingInterval = 5L;
        if (rank <= 100) {
            pollingInterval = 1L;
        } else if (rank <= 1000) {
            pollingInterval = 3L;
        }
        return new QueuePosition(userId, QueueStatus.WAITING, rank, estimatedWaitTime, pollingInterval, null);
    }

    public static QueuePosition active(Long userId, String token) {
        return new QueuePosition(userId, QueueStatus.ACTIVE, null, null, null, token);
    }

    public static QueuePosition none(Long userId) {
        return new QueuePosition(userId, QueueStatus.NONE, null, null, null, null);
    }
}
