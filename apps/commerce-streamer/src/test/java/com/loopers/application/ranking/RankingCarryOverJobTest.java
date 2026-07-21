package com.loopers.application.ranking;

import com.loopers.domain.ranking.RankingKeyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RankingCarryOverJobTest {

    private final RankingRedisRepository rankingRedisRepository = mock(RankingRedisRepository.class);
    private final RankingCarryOverProductRepository productRepository = mock(RankingCarryOverProductRepository.class);
    private final RankingCarryOverJob rankingCarryOverJob = new RankingCarryOverJob(
        rankingRedisRepository,
        productRepository,
        new RankingKeyPolicy()
    );

    @Test
    @DisplayName("전일 Top 1,000 랭킹 점수의 10%를 오늘 랭킹에 적재하고 done key를 기록한다.")
    void carryOver_ShouldApplyTenPercentOfYesterdayTopRankings() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 5);
        given(rankingRedisRepository.isCarryOverDone("20260714")).willReturn(false);
        given(rankingRedisRepository.findTopRankings("20260713", 1000))
            .willReturn(List.of(
                new RankingScoreEntry(1L, 100.0),
                new RankingScoreEntry(2L, 50.0)
            ));
        given(productRepository.findAvailableProductIds(List.of(1L, 2L)))
            .willReturn(List.of(1L, 2L));

        // when
        rankingCarryOverJob.carryOver(now);

        // then
        verify(rankingRedisRepository).incrementCarryOverScore("20260714", 1L, 10.0);
        verify(rankingRedisRepository).incrementCarryOverScore("20260714", 2L, 5.0);
        verify(rankingRedisRepository).markCarryOverDone("20260714");
    }

    @Test
    @DisplayName("done key가 이미 있으면 carry over를 다시 수행하지 않는다.")
    void carryOver_WhenAlreadyDone_ShouldSkip() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 5);
        given(rankingRedisRepository.isCarryOverDone("20260714")).willReturn(true);

        // when
        rankingCarryOverJob.carryOver(now);

        // then
        verify(rankingRedisRepository, never()).findTopRankings("20260713", 1000);
    }

    @Test
    @DisplayName("00:10 이후에는 carry over를 수행하지 않는다.")
    void carryOver_WhenAfterCutoff_ShouldSkip() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 11);

        // when
        rankingCarryOverJob.carryOver(now);

        // then
        verify(rankingRedisRepository, never()).isCarryOverDone("20260714");
    }

    @Test
    @DisplayName("전일 랭킹이 비어 있으면 점수 적재 없이 done key만 기록한다.")
    void carryOver_WhenYesterdayRankingIsEmpty_ShouldOnlyMarkDone() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 5);
        given(rankingRedisRepository.isCarryOverDone("20260714")).willReturn(false);
        given(rankingRedisRepository.findTopRankings("20260713", 1000)).willReturn(List.of());

        // when
        rankingCarryOverJob.carryOver(now);

        // then
        verify(rankingRedisRepository, never()).incrementCarryOverScore("20260714", 1L, 10.0);
        verify(rankingRedisRepository).markCarryOverDone("20260714");
    }

    @Test
    @DisplayName("삭제 또는 미존재 상품은 carry over 대상에서 제외한다.")
    void carryOver_ShouldExcludeUnavailableProducts() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 5);
        given(rankingRedisRepository.isCarryOverDone("20260714")).willReturn(false);
        given(rankingRedisRepository.findTopRankings("20260713", 1000))
            .willReturn(List.of(
                new RankingScoreEntry(1L, 100.0),
                new RankingScoreEntry(2L, 50.0)
            ));
        given(productRepository.findAvailableProductIds(List.of(1L, 2L)))
            .willReturn(List.of(1L));

        // when
        rankingCarryOverJob.carryOver(now);

        // then
        verify(rankingRedisRepository).incrementCarryOverScore("20260714", 1L, 10.0);
        verify(rankingRedisRepository, never()).incrementCarryOverScore("20260714", 2L, 5.0);
        verify(rankingRedisRepository).markCarryOverDone("20260714");
    }

    @Test
    @DisplayName("상품 DB 조회 예외는 carry over 실패로 처리하고 done key를 기록하지 않는다.")
    void carryOver_WhenProductRepositoryFails_ShouldFailWithoutDoneKey() {
        // given
        LocalDateTime now = LocalDateTime.of(2026, 7, 14, 0, 5);
        RuntimeException exception = new RuntimeException("db failure");
        given(rankingRedisRepository.isCarryOverDone("20260714")).willReturn(false);
        given(rankingRedisRepository.findTopRankings("20260713", 1000))
            .willReturn(List.of(new RankingScoreEntry(1L, 100.0)));
        given(productRepository.findAvailableProductIds(List.of(1L))).willThrow(exception);

        // then
        assertThatThrownBy(() -> rankingCarryOverJob.carryOver(now))
            .isSameAs(exception);
        verify(rankingRedisRepository, never()).markCarryOverDone("20260714");
    }
}
