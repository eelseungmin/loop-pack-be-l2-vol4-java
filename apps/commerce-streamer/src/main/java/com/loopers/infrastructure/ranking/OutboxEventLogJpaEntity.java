package com.loopers.infrastructure.ranking;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@Entity
@Table(name = "outbox_events")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEventLogJpaEntity {

    @Id
    private Long id;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    OutboxEventLogJpaEntity(
        Long id,
        String eventType,
        String status,
        String payload,
        LocalDateTime createdAt
    ) {
        this.id = id;
        this.eventType = eventType;
        this.status = status;
        this.payload = payload;
        this.createdAt = createdAt;
    }
}
