package com.loopers.infrastructure.outbox;

import com.loopers.domain.outbox.EventType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class UserActionLogOutboxProcessor implements OutboxProcessor {

    @Override
    public boolean supports(EventType eventType) {
        return eventType == EventType.USER_ACTION_LOG;
    }

    @Override
    public void process(String payload) throws Exception {
        log.info("[OUTBOX-REPROCESS] Reprocessing user action log. Payload: {}", payload);
    }
}
