package com.loopers.infrastructure.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RankingRebuildEventRepositoryImplTest {

    @Test
    @DisplayName("PRODUCT_RANKING_EVENT 중 INIT, COMPLETED 상태를 조회하고 Outbox id를 eventId로 주입한다.")
    void findEventsForRebuild_ShouldReadRankingEventsAndInjectOutboxId() {
        // given
        OutboxEventLogJpaRepository outboxEventLogJpaRepository = mock(OutboxEventLogJpaRepository.class);
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        RankingRebuildEventRepositoryImpl repository = new RankingRebuildEventRepositoryImpl(
            outboxEventLogJpaRepository,
            objectMapper
        );
        LocalDateTime from = LocalDateTime.of(2026, 7, 13, 0, 0);
        LocalDateTime to = LocalDateTime.of(2026, 7, 15, 0, 0);

        when(outboxEventLogJpaRepository.findAllByEventTypeAndStatusInAndCreatedAtGreaterThanEqual(
            "PRODUCT_RANKING_EVENT",
            List.of("INIT", "COMPLETED"),
            from.minusDays(1)
        )).thenReturn(List.of(
            new OutboxEventLogJpaEntity(
                1L,
                "PRODUCT_RANKING_EVENT",
                "INIT",
                """
                    {"rankingEventType":"VIEW","productId":1,"price":0,"amount":0,"occurredAt":"2026-07-14T10:00:00"}
                    """,
                LocalDateTime.of(2026, 7, 14, 10, 0)
            ),
            new OutboxEventLogJpaEntity(
                2L,
                "PRODUCT_RANKING_EVENT",
                "COMPLETED",
                """
                    {"rankingEventType":"ORDER","productId":2,"price":10000,"amount":2,"occurredAt":"2026-07-13T10:00:00"}
                    """,
                LocalDateTime.of(2026, 7, 13, 10, 0)
            ),
            new OutboxEventLogJpaEntity(
                3L,
                "PRODUCT_RANKING_EVENT",
                "INIT",
                """
                    {"rankingEventType":"LIKE","productId":3,"price":0,"amount":0,"occurredAt":"2026-07-12T10:00:00"}
                    """,
                LocalDateTime.of(2026, 7, 12, 10, 0)
            )
        ));

        // when
        List<ProductRankingEvent> events = repository.findEventsForRebuild(from, to);

        // then
        assertThat(events).hasSize(2);
        assertThat(events.get(0).eventId()).isEqualTo("1");
        assertThat(events.get(0).eventType()).isEqualTo(RankingEventType.VIEW);
        assertThat(events.get(0).productId()).isEqualTo(1L);
        assertThat(events.get(1).eventId()).isEqualTo("2");
        assertThat(events.get(1).eventType()).isEqualTo(RankingEventType.ORDER);
        assertThat(events.get(1).amount()).isEqualTo(2);

        verify(outboxEventLogJpaRepository).findAllByEventTypeAndStatusInAndCreatedAtGreaterThanEqual(
            "PRODUCT_RANKING_EVENT",
            List.of("INIT", "COMPLETED"),
            from.minusDays(1)
        );
    }
}
