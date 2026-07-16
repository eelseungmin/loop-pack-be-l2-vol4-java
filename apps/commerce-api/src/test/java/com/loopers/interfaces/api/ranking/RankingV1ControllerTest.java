package com.loopers.interfaces.api.ranking;

import com.loopers.application.ranking.RankingFacade;
import com.loopers.application.ranking.RankingProductInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RankingV1Controller.class)
class RankingV1ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private RankingFacade rankingFacade;

    @Test
    @DisplayName("랭킹 페이지 조회 API 요청 시 상품 정보가 포함된 랭킹 PageResponse를 반환한다.")
    void getRankings_ShouldReturnRankingPage() throws Exception {
        // given
        RankingProductInfo info = new RankingProductInfo(
            1,
            10.0,
            1L,
            "Air Max",
            10L,
            "Nike",
            new BigDecimal("1000.0000")
        );
        given(rankingFacade.getRankings("20260714", 1, 20))
            .willReturn(new PageImpl<>(List.of(info), PageRequest.of(0, 20), 1));

        // when & then
        mockMvc.perform(get("/api/v1/rankings")
                .param("date", "20260714")
                .param("page", "1")
                .param("size", "20")
                .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.meta.result").value("SUCCESS"))
            .andExpect(jsonPath("$.data.content[0].rank").value(1))
            .andExpect(jsonPath("$.data.content[0].score").value(10.0))
            .andExpect(jsonPath("$.data.content[0].productId").value(1))
            .andExpect(jsonPath("$.data.content[0].productName").value("Air Max"))
            .andExpect(jsonPath("$.data.content[0].brandName").value("Nike"))
            .andExpect(jsonPath("$.data.content[0].price").value(1000.0000))
            .andExpect(jsonPath("$.data.pageNumber").value(0))
            .andExpect(jsonPath("$.data.pageSize").value(20))
            .andExpect(jsonPath("$.data.totalElements").value(1));
    }
}
