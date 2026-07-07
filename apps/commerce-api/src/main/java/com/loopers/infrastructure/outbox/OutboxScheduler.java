package com.loopers.infrastructure.outbox;

import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxScheduler {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 3000)
    public void run() {
        log.info("Starting Outbox reprocessing scheduler...");

        List<OutboxEvent> initEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        log.info("Found {} INIT outbox events to reprocess.", initEvents.size());

        for (OutboxEvent event : initEvents) {
            try {
                log.info("Reprocessing outbox event: id={}, type={}", event.getId(), event.getEventType());
                event.increaseRetryCount();

                String topic = event.getEventType().name();
                String key = event.getId().toString();
                String payload = event.getPayload();

                kafkaTemplate.send(topic, key, payload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                event.complete();
                                outboxEventRepository.save(event);
                                log.info("Successfully reprocessed outbox event: id={}", event.getId());
                            } else {
                                handleFailure(event, ex);
                            }
                        });
            } catch (Exception e) {
                handleFailure(event, e);
            }
        }

        log.info("Outbox reprocessing scheduler finished.");
    }

    private void handleFailure(OutboxEvent event, Throwable e) {
        log.error("Failed to reprocess outbox event: id={}", event.getId(), e);
        if (event.getRetryCount() >= 3) {
            event.fail(e.getMessage() != null ? e.getMessage() : "Exceeded maximum retry counts (3)");
        } else {
            event.recordError(e.getMessage());
        }
        outboxEventRepository.save(event);
    }
}
