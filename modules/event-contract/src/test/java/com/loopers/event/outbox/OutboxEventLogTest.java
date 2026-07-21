package com.loopers.event.outbox;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventLogTest {

    @Test
    @DisplayName("Outbox 이벤트 로그는 재빌드 조회에 필요한 읽기 전용 메타데이터를 가진다.")
    void outboxEventLog_ShouldExposeReadOnlyMetadataForRebuild() {
        // given
        LocalDateTime createdAt = LocalDateTime.of(2026, 7, 14, 10, 0);

        // when
        OutboxEventLog log = new OutboxEventLog(
            1L,
            "PRODUCT_RANKING_EVENT",
            "INIT",
            "{\"rankingEventType\":\"VIEW\"}",
            createdAt
        );

        // then
        assertThat(log.id()).isEqualTo(1L);
        assertThat(log.eventType()).isEqualTo("PRODUCT_RANKING_EVENT");
        assertThat(log.status()).isEqualTo("INIT");
        assertThat(log.payload()).isEqualTo("{\"rankingEventType\":\"VIEW\"}");
        assertThat(log.createdAt()).isEqualTo(createdAt);
    }
}
