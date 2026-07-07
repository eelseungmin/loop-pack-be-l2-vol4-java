package com.loopers.application.user;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.user.UserActionLevel;
import com.loopers.domain.user.UserActionLogEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserActionEventListener {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    @Async
    @EventListener
    public void handleUserActionLog(UserActionLogEvent event) {
        log.info("UserAction Log Event received: userId={}, action={}, level={}",
                event.userId(), event.action(), event.level());

        if (event.level() == UserActionLevel.LOW) {
            // Simple logging (Fire-and-Forget)
            log.info("[FIRE-AND-FORGET] User action LOW log: {}", event.payload());
        } else if (event.level() == UserActionLevel.HIGH) {
            // Core logging (Outbox guaranteed delivery)
            log.info("[OUTBOX] User action HIGH log: {}", event.payload());
            saveToOutbox(event);
        }
    }

    private void saveToOutbox(UserActionLogEvent event) {
        try {
            transactionTemplate.executeWithoutResult(status -> {
                try {
                    String payload = objectMapper.writeValueAsString(event);
                    OutboxEvent outboxEvent = new OutboxEvent(EventType.USER_ACTION_LOG, payload, OutboxEventStatus.INIT);
                    outboxEventRepository.save(outboxEvent);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            log.info("Successfully saved UserActionLogEvent (HIGH) to outbox.");
        } catch (Exception ex) {
            log.error("Failed to save UserActionLogEvent (HIGH) to outbox.", ex);
        }
    }
}
