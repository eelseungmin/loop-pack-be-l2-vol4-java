package com.loopers.infrastructure.outbox;

import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxEventRepository outboxEventRepository;
    private final List<OutboxProcessor> outboxProcessors;

    @Scheduled(cron = "0 */5 * * * *")
    public void run() {
        log.info("Starting Outbox reprocessing scheduler...");

        List<OutboxEvent> initEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        log.info("Found {} INIT outbox events to reprocess.", initEvents.size());

        for (OutboxEvent event : initEvents) {
            try {
                log.info("Reprocessing outbox event: id={}, type={}", event.getId(), event.getEventType());
                event.increaseRetryCount();

                OutboxProcessor processor = outboxProcessors.stream()
                        .filter(p -> p.supports(event.getEventType()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("No processor supported for event type: " + event.getEventType()));

                processor.process(event.getPayload());

                event.complete();
                outboxEventRepository.save(event);
                log.info("Successfully reprocessed outbox event: id={}", event.getId());
            } catch (Exception e) {
                log.error("Failed to reprocess outbox event: id={}", event.getId(), e);
                if (event.getRetryCount() >= 3) {
                    event.fail(e.getMessage() != null ? e.getMessage() : "Exceeded maximum retry counts (3)");
                } else {
                    event.recordError(e.getMessage());
                }
                outboxEventRepository.save(event);
            }
        }

        log.info("Outbox reprocessing scheduler finished.");
    }
}
