package com.loopers.interfaces.consumer;

import com.loopers.application.metrics.MetricsUpdateService;
import com.loopers.domain.ranking.ProductRankingEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MetricsKafkaConsumer {

    private final MetricsUpdateService metricsUpdateService;

    @KafkaListener(
        topics = "${ranking.kafka.topic:product-events}",
        groupId = "commerce-streamer-metrics-group"
    )
    public void metricsListener(ProductRankingEvent event, Acknowledgment acknowledgment) {
        metricsUpdateService.update(event);
        acknowledgment.acknowledge();
    }
}
