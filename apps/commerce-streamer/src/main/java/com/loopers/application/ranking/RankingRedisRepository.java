package com.loopers.application.ranking;

import java.time.LocalDateTime;
import java.util.List;

public interface RankingRedisRepository {

    boolean incrementScoreIfFirstHandled(String eventId, String dateKey, Long productId, double score);

    void removeProductFromRecentRankings(Long productId, LocalDateTime deletedAt);

    void clearRebuildRanking(String dateKey);

    void incrementRebuildScore(String dateKey, Long productId, double score);

    void removeProductFromRecentRebuildRankings(Long productId, LocalDateTime deletedAt);

    void replaceRankingWithRebuild(String dateKey);

    List<RankingScoreEntry> findTopRankings(String dateKey, int limit);

    void incrementCarryOverScore(String dateKey, Long productId, double score);

    boolean isCarryOverDone(String dateKey);

    void markCarryOverDone(String dateKey);
}
