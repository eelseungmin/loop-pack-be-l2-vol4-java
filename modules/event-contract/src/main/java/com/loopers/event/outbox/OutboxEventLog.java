package com.loopers.event.outbox;

import java.time.LocalDateTime;

public record OutboxEventLog(
    Long id,
    String eventType,
    String status,
    String payload,
    LocalDateTime createdAt
) {
}
