package com.loopers.domain.ranking;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record ProductRankingEvent(
    String eventId,
    RankingEventType rankingEventType,
    Long productId,
    BigDecimal price,
    int amount,
    LocalDateTime occurredAt
) {

    public RankingEventType eventType() {
        return rankingEventType;
    }
}
