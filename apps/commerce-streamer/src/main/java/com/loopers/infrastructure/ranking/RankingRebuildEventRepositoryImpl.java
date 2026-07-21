package com.loopers.infrastructure.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.ranking.RankingRebuildEventRepository;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.event.outbox.OutboxEventLog;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RankingRebuildEventRepositoryImpl implements RankingRebuildEventRepository {

    private static final String PRODUCT_RANKING_EVENT = "PRODUCT_RANKING_EVENT";
    private static final List<String> REBUILD_TARGET_STATUSES = List.of("INIT", "COMPLETED");
    private static final int CREATED_AT_LOOKBACK_DAYS = 1;

    private final OutboxEventLogJpaRepository outboxEventLogJpaRepository;
    private final ObjectMapper objectMapper;

    @Override
    public List<ProductRankingEvent> findEventsForRebuild(LocalDateTime from, LocalDateTime to) {
        return outboxEventLogJpaRepository.findAllByEventTypeAndStatusInAndCreatedAtGreaterThanEqual(
                PRODUCT_RANKING_EVENT,
                REBUILD_TARGET_STATUSES,
                from.minusDays(CREATED_AT_LOOKBACK_DAYS)
            )
            .stream()
            .map(this::toOutboxEventLog)
            .map(this::toRankingRebuildEvent)
            .filter(event -> !event.occurredAt().isBefore(from))
            .filter(event -> event.occurredAt().isBefore(to))
            .toList();
    }

    private OutboxEventLog toOutboxEventLog(OutboxEventLogJpaEntity entity) {
        return new OutboxEventLog(
            entity.getId(),
            entity.getEventType(),
            entity.getStatus(),
            entity.getPayload(),
            entity.getCreatedAt()
        );
    }

    private ProductRankingEvent toRankingRebuildEvent(OutboxEventLog eventLog) {
        try {
            RankingEventPayload payload = objectMapper.readValue(eventLog.payload(), RankingEventPayload.class);
            return new ProductRankingEvent(
                String.valueOf(eventLog.id()),
                payload.rankingEventType(),
                payload.productId(),
                payload.price(),
                payload.amount(),
                payload.occurredAt()
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse product ranking event payload.", e);
        }
    }

    private record RankingEventPayload(
        RankingEventType rankingEventType,
        Long productId,
        BigDecimal price,
        int amount,
        LocalDateTime occurredAt
    ) {
    }
}
