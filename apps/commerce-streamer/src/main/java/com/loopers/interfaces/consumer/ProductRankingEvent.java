package com.loopers.interfaces.consumer;

import com.loopers.domain.ranking.RankingEventType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record ProductRankingEvent(
    String eventId,
    RankingEventType eventType,
    Long productId,
    BigDecimal price,
    int amount,
    LocalDateTime occurredAt
) {
}
