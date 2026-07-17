package com.loopers.application.ranking;

import java.util.List;

public interface RankingCarryOverProductRepository {

    List<Long> findAvailableProductIds(List<Long> productIds);
}
