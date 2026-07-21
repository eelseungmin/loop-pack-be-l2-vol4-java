package com.loopers.interfaces.consumer;

import com.loopers.application.metrics.MetricsUpdateService;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingEventType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MetricsKafkaConsumerTest {

    @Test
    @DisplayName("상품 이벤트를 수신하면 MetricsUpdateService만 호출하고 Ack한다.")
    void metricsListener_ShouldUpdateMetricsAndAcknowledge() {
        // given
        MetricsUpdateService metricsUpdateService = mock(MetricsUpdateService.class);
        MetricsKafkaConsumer metricsKafkaConsumer = new MetricsKafkaConsumer(metricsUpdateService);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        ProductRankingEvent event = new ProductRankingEvent(
            "event-1",
            RankingEventType.VIEW,
            1L,
            BigDecimal.ZERO,
            0,
            LocalDateTime.of(2026, 7, 14, 10, 0)
        );

        // when
        metricsKafkaConsumer.metricsListener(event, acknowledgment);

        // then
        verify(metricsUpdateService).update(event);
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("RankingKafkaConsumer와 다른 Consumer Group으로 같은 기본 토픽을 소비한다.")
    void metricsListener_ShouldUseDifferentConsumerGroupFromRankingConsumer() throws NoSuchMethodException {
        // given
        KafkaListener metricsListener = MetricsKafkaConsumer.class
            .getMethod("metricsListener", ProductRankingEvent.class, Acknowledgment.class)
            .getAnnotation(KafkaListener.class);
        KafkaListener rankingListener = RankingKafkaConsumer.class
            .getMethod("rankingListener", ProductRankingEvent.class, Acknowledgment.class)
            .getAnnotation(KafkaListener.class);

        // then
        assertThat(metricsListener.topics()).containsExactly("${ranking.kafka.topic:product-events}");
        assertThat(rankingListener.topics()).containsExactly("${ranking.kafka.topic:product-events}");
        assertThat(metricsListener.groupId()).isEqualTo("commerce-streamer-metrics-group");
        assertThat(rankingListener.groupId()).isEqualTo("commerce-streamer-ranking-group");
    }
}
