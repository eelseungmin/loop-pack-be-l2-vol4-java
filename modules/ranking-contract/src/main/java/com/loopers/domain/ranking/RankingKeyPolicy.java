package com.loopers.domain.ranking;

import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class RankingKeyPolicy {

    private static final String RANKING_KEY_PREFIX = "ranking:all:";
    private static final String REBUILD_RANKING_KEY_PREFIX = "ranking:rebuild:all:";
    private static final String HANDLED_KEY_PREFIX = "ranking:handled:";
    private static final String CARRY_OVER_DONE_KEY_PREFIX = "ranking:carry-over:done:";
    private static final DateTimeFormatter DATE_KEY_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    public String dateKey(LocalDateTime occurredAt) {
        return occurredAt.format(DATE_KEY_FORMATTER);
    }

    public String rankingKey(String dateKey) {
        return RANKING_KEY_PREFIX + dateKey;
    }

    public String rebuildRankingKey(String dateKey) {
        return REBUILD_RANKING_KEY_PREFIX + dateKey;
    }

    public String handledKey(String dateKey) {
        return HANDLED_KEY_PREFIX + dateKey;
    }

    public String carryOverDoneKey(String dateKey) {
        return CARRY_OVER_DONE_KEY_PREFIX + dateKey;
    }
}
