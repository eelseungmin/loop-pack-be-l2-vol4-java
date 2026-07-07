package com.loopers.infrastructure.outbox;

import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.OutboxEventCreatedEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxKafkaRelayListener {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Async
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOutboxEventCreated(OutboxEventCreatedEvent event) {
        log.info("Relaying OutboxEvent to Kafka. outboxEventId: {}", event.outboxEventId());

        outboxEventRepository.findById(event.outboxEventId()).ifPresent(outboxEvent -> {
            if (outboxEvent.getStatus() != OutboxEventStatus.INIT) {
                return;
            }

            String topic = outboxEvent.getEventType().name();
            String key = outboxEvent.getId().toString();
            String payload = outboxEvent.getPayload();

            kafkaTemplate.send(topic, key, payload)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            outboxEvent.complete();
                            outboxEventRepository.save(outboxEvent);
                            log.info("Successfully relayed OutboxEvent: {}", outboxEvent.getId());
                        } else {
                            log.error("Failed to relay OutboxEvent: {}", outboxEvent.getId(), ex);
                            outboxEvent.recordError(ex.getMessage());
                            outboxEventRepository.save(outboxEvent);
                        }
                    });
        });
    }
}
