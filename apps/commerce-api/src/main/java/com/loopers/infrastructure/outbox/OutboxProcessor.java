package com.loopers.infrastructure.outbox;

import com.loopers.domain.outbox.EventType;

public interface OutboxProcessor {
    boolean supports(EventType eventType);
    void process(String payload) throws Exception;
}
