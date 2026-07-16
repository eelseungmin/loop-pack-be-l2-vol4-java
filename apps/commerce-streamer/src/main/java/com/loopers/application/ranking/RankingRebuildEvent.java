package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingEventType;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record RankingRebuildEvent(
    String eventId,
    RankingEventType eventType,
    Long productId,
    BigDecimal price,
    int amount,
    LocalDateTime occurredAt
) {
}
