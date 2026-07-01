package com.loopers.application.outbox;

import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;

import java.util.List;
import java.util.Optional;

public interface OutboxEventRepository {
    OutboxEvent save(OutboxEvent outboxEvent);
    Optional<OutboxEvent> findById(Long id);
    List<OutboxEvent> findAllByStatus(OutboxEventStatus status);
}
