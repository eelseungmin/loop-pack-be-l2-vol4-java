package com.loopers.interfaces.api.ranking;

import com.loopers.application.ranking.RankingFacade;
import com.loopers.application.ranking.RankingProductInfo;
import com.loopers.interfaces.api.ApiResponse;
import com.loopers.interfaces.api.PageResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/rankings")
public class RankingV1Controller {

    private final RankingFacade rankingFacade;

    @GetMapping
    public ApiResponse<PageResponse<RankingV1Dto.RankingResponse>> getRankings(
        @RequestParam(value = "date") String date,
        @RequestParam(value = "page", defaultValue = "1") int page,
        @RequestParam(value = "size", defaultValue = "20") int size
    ) {
        Page<RankingProductInfo> rankingPage = rankingFacade.getRankings(date, page, size);
        Page<RankingV1Dto.RankingResponse> responsePage = rankingPage.map(RankingV1Dto.RankingResponse::from);
        return ApiResponse.success(PageResponse.from(responsePage));
    }
}
