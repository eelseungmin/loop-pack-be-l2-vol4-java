package com.loopers.application.ranking;

public record RankingEntry(
    Long productId,
    long rank,
    double score
) {
}
