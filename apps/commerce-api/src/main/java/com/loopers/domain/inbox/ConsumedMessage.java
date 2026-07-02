package com.loopers.domain.inbox;

import com.loopers.domain.BaseTimeEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "consumed_messages")
public class ConsumedMessage extends BaseTimeEntity {

    @Id
    @Column(name = "message_id", nullable = false)
    private String messageId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;

    public ConsumedMessage(String messageId, String topic) {
        this.messageId = messageId;
        this.topic = topic;
        this.processedAt = LocalDateTime.now();
    }
}
