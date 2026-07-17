package com.loopers.infrastructure.ranking;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RankingCarryOverProductRepositoryImplTest {

    @Test
    @DisplayName("논리 삭제되지 않은 상품 ID만 조회한다.")
    void findAvailableProductIds_ShouldReturnOnlyNotDeletedProducts() {
        // given
        NamedParameterJdbcTemplate jdbcTemplate = mock(NamedParameterJdbcTemplate.class);
        RankingCarryOverProductRepositoryImpl repository = new RankingCarryOverProductRepositoryImpl(jdbcTemplate);
        given(jdbcTemplate.queryForList(
            eq("SELECT id FROM product WHERE id IN (:ids) AND is_deleted = false"),
            any(SqlParameterSource.class),
            eq(Long.class)
        )).willReturn(List.of(1L, 3L));

        // when
        List<Long> result = repository.findAvailableProductIds(List.of(1L, 2L, 3L));

        // then
        assertThat(result).containsExactly(1L, 3L);
    }

    @Test
    @DisplayName("조회 대상 상품 ID가 없으면 DB를 조회하지 않는다.")
    void findAvailableProductIds_WhenEmpty_ShouldNotQueryDatabase() {
        // given
        NamedParameterJdbcTemplate jdbcTemplate = mock(NamedParameterJdbcTemplate.class);
        RankingCarryOverProductRepositoryImpl repository = new RankingCarryOverProductRepositoryImpl(jdbcTemplate);

        // when
        List<Long> result = repository.findAvailableProductIds(List.of());

        // then
        assertThat(result).isEmpty();
        verify(jdbcTemplate, never()).queryForList(any(String.class), any(SqlParameterSource.class), eq(Long.class));
    }
}
