package com.loopers.infrastructure.ranking;

import com.loopers.application.ranking.RankingCarryOverProductRepository;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RankingCarryOverProductRepositoryImpl implements RankingCarryOverProductRepository {

    private static final String FIND_AVAILABLE_PRODUCT_IDS_SQL =
        "SELECT id FROM product WHERE id IN (:ids) AND is_deleted = false";

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public RankingCarryOverProductRepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<Long> findAvailableProductIds(List<Long> productIds) {
        if (productIds.isEmpty()) {
            return List.of();
        }

        MapSqlParameterSource parameters = new MapSqlParameterSource("ids", productIds);
        return jdbcTemplate.queryForList(FIND_AVAILABLE_PRODUCT_IDS_SQL, parameters, Long.class);
    }
}
