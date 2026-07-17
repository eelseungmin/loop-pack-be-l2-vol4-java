package com.loopers.application.ranking;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class RankingCarryOverScheduler {

    private final RankingCarryOverJob rankingCarryOverJob;

    public RankingCarryOverScheduler(RankingCarryOverJob rankingCarryOverJob) {
        this.rankingCarryOverJob = rankingCarryOverJob;
    }

    @Scheduled(cron = "0 0-10 0 * * *")
    public void carryOver() {
        rankingCarryOverJob.carryOver(LocalDateTime.now());
    }
}
