package com.loopers.domain.queue;

public record QueuePosition(
        Long userId,
        QueueStatus status,
        Long rank,
        Long estimatedWaitTime,
        String token
) {
    public static QueuePosition waiting(Long userId, Long rank, Long estimatedWaitTime) {
        return new QueuePosition(userId, QueueStatus.WAITING, rank, estimatedWaitTime, null);
    }

    public static QueuePosition active(Long userId, String token) {
        return new QueuePosition(userId, QueueStatus.ACTIVE, null, null, token);
    }

    public static QueuePosition none(Long userId) {
        return new QueuePosition(userId, QueueStatus.NONE, null, null, null);
    }
}
