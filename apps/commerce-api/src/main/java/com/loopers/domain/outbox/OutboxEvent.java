package com.loopers.domain.outbox;

import com.loopers.domain.BaseTimeEntity;
import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(
        name = "outbox_events",
        indexes = {
                @Index(name = "idx_outbox_events_status", columnList = "status")
        })
public class OutboxEvent extends BaseTimeEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false)
    private EventType eventType;

    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxEventStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Column(name = "error_message")
    private String errorMessage;

    public OutboxEvent(EventType eventType, String payload, OutboxEventStatus status) {
        this.eventType = eventType;
        this.payload = payload;
        this.status = status;
        this.retryCount = 0;
    }

    public void complete() {
        this.status = OutboxEventStatus.COMPLETED;
    }

    public void fail(String errorMessage) {
        this.status = OutboxEventStatus.FAILED;
        this.errorMessage = errorMessage;
    }

    public void recordError(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public void increaseRetryCount() {
        this.retryCount++;
    }
}
