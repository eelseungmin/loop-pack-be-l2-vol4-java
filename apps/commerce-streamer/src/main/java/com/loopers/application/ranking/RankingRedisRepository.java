package com.loopers.application.ranking;

import java.time.LocalDateTime;

public interface RankingRedisRepository {

    boolean incrementScoreIfFirstHandled(String eventId, String dateKey, Long productId, double score);

    void removeProductFromRecentRankings(Long productId, LocalDateTime deletedAt);

    void clearRebuildRanking(String dateKey);

    void incrementRebuildScore(String dateKey, Long productId, double score);

    void removeProductFromRecentRebuildRankings(Long productId, LocalDateTime deletedAt);

    void replaceRankingWithRebuild(String dateKey);
}
