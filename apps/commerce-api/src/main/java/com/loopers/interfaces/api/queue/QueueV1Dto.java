package com.loopers.interfaces.api.queue;

import com.loopers.domain.queue.QueueStatus;

public class QueueV1Dto {

    public record QueueEnterResponse(
            QueueStatus status,
            Long userId,
            Long rank,
            Long estimatedWaitTime
    ) {}

    public record QueuePositionResponse(
            QueueStatus status,
            Long userId,
            Long rank,
            Long estimatedWaitTime,
            String token
    ) {}
}
