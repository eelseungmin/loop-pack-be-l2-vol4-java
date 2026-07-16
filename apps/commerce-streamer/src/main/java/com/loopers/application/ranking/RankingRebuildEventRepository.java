package com.loopers.application.ranking;

import java.time.LocalDateTime;
import java.util.List;

public interface RankingRebuildEventRepository {

    List<RankingRebuildEvent> findEventsForRebuild(LocalDateTime from, LocalDateTime to);
}
