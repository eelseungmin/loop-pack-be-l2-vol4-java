package com.loopers.interfaces.consumer;

import com.loopers.application.ranking.RankingRedisRepository;
import com.loopers.domain.ranking.ProductRankingEvent;
import com.loopers.domain.ranking.RankingKeyPolicy;
import com.loopers.domain.ranking.RankingEventType;
import com.loopers.domain.ranking.RankingScorePolicy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class RankingKafkaConsumer {

    private final RankingScorePolicy rankingScorePolicy;
    private final RankingKeyPolicy rankingKeyPolicy;
    private final RankingRedisRepository rankingRedisRepository;

    public RankingKafkaConsumer(
        RankingScorePolicy rankingScorePolicy,
        RankingKeyPolicy rankingKeyPolicy,
        RankingRedisRepository rankingRedisRepository
    ) {
        this.rankingScorePolicy = rankingScorePolicy;
        this.rankingKeyPolicy = rankingKeyPolicy;
        this.rankingRedisRepository = rankingRedisRepository;
    }

    @KafkaListener(
        topics = "${ranking.kafka.topic:product-events}",
        groupId = "commerce-streamer-ranking-group"
    )
    public void rankingListener(ProductRankingEvent event, Acknowledgment acknowledgment) {
        if (event.eventType() == RankingEventType.PRODUCT_DELETED) {
            rankingRedisRepository.removeProductFromRecentRankings(event.productId(), event.occurredAt());
            acknowledgment.acknowledge();
            return;
        }

        String dateKey = rankingKeyPolicy.dateKey(event.occurredAt());
        double score = rankingScorePolicy.calculateScore(event.eventType(), event.price(), event.amount());

        rankingRedisRepository.incrementScoreIfFirstHandled(
            event.eventId(),
            dateKey,
            event.productId(),
            score
        );
        acknowledgment.acknowledge();
    }
}
