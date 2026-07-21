package com.loopers.interfaces.api.product;

import com.loopers.application.product.ProductFacade;
import com.loopers.application.product.ProductInfo;
import com.loopers.application.ranking.ProductRankingInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProductV1Controller.class)
class ProductV1ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ProductFacade productFacade;

    @Test
    @DisplayName("상품 페이지 조회 API 요청 시 올바른 페이지 정보가 담긴 PageResponse가 반환된다.")
    void getProducts_ApiSuccess() throws Exception {
        // given
        Long brandId = 10L;
        String sort = "likes_desc";
        int page = 0;
        int size = 10;
        Pageable pageable = PageRequest.of(page, size);

        ProductInfo info = new ProductInfo(
                1L,
                brandId,
                "Nike",
                "Air Max",
                new BigDecimal("1000.0000"),
                15,
                ZonedDateTime.now()
        );

        given(productFacade.getProducts(eq(brandId), eq(sort), any(Pageable.class)))
                .willReturn(new PageImpl<>(List.of(info), pageable, 1));

        // when & then
        mockMvc.perform(get("/api/v1/products")
                        .param("brandId", String.valueOf(brandId))
                        .param("sort", sort)
                        .param("page", String.valueOf(page))
                        .param("size", String.valueOf(size))
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.result").value("SUCCESS"))
                .andExpect(jsonPath("$.data.content[0].id").value(1))
                .andExpect(jsonPath("$.data.content[0].brandName").value("Nike"))
                .andExpect(jsonPath("$.data.content[0].likeCount").value(15))
                .andExpect(jsonPath("$.data.pageNumber").value(page))
                .andExpect(jsonPath("$.data.pageSize").value(size))
                .andExpect(jsonPath("$.data.totalElements").value(1))
                .andExpect(jsonPath("$.data.totalPages").value(1))
                .andExpect(jsonPath("$.data.last").value(true));
    }

    @Test
    @DisplayName("상품 상세 조회 API 응답에 오늘 기준 랭킹 정보가 포함된다.")
    void getProduct_WhenRankingExists_ShouldReturnRankingInfo() throws Exception {
        // given
        Long productId = 1L;
        ProductInfo info = new ProductInfo(
                productId,
                10L,
                "Nike",
                "Air Max",
                new BigDecimal("1000.0000"),
                15,
                ZonedDateTime.now(),
                new ProductRankingInfo("20260716", 3, 12.5)
        );

        given(productFacade.getProduct(productId)).willReturn(info);

        // when & then
        mockMvc.perform(get("/api/v1/products/{productId}", productId)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.result").value("SUCCESS"))
                .andExpect(jsonPath("$.data.id").value(1))
                .andExpect(jsonPath("$.data.ranking.date").value("20260716"))
                .andExpect(jsonPath("$.data.ranking.rank").value(3))
                .andExpect(jsonPath("$.data.ranking.score").value(12.5));
    }
}
