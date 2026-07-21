package com.loopers.application.ranking;

import java.util.List;
import java.util.Optional;

public interface RankingRedisRepository {

    List<RankingEntry> findRankings(String dateKey, int page, int size);

    Optional<ProductRankingInfo> findProductRanking(String dateKey, Long productId);

    long count(String dateKey);
}
