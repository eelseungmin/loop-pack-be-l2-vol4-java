package com.loopers.application.ranking;

import com.loopers.domain.ranking.ProductRankingEvent;

import java.time.LocalDateTime;
import java.util.List;

public interface RankingRebuildEventRepository {

    List<ProductRankingEvent> findEventsForRebuild(LocalDateTime from, LocalDateTime to);
}
