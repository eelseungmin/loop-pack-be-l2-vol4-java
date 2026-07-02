package com.loopers.interfaces.api.coupon;

import com.loopers.application.coupon.CouponFacade;
import com.loopers.domain.coupon.CouponType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(CouponV1Controller.class)
class CouponV1ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private CouponFacade couponFacade;

    @Test
    @DisplayName("쿠폰 발급 요청 시 200 OK를 반환하고 Facade를 호출한다.")
    void issueCoupon_ShouldReturnOk() throws Exception {
        // given
        Long userId = 1L;
        Long couponId = 10L;

        // when & then
        mockMvc.perform(post("/api/v1/coupons/{couponId}/issue", couponId)
                        .header("X-Loopers-UserId", userId))
                .andExpect(status().isOk());

        verify(couponFacade).issueCoupon(userId, couponId);
    }

    @Test
    @DisplayName("내 쿠폰 목록 조회 시 200 OK와 목록을 반환한다.")
    void getUsersCoupons_ShouldReturnOkAndList() throws Exception {
        // given
        Long userId = 1L;
        List<CouponV1Dto.UserCouponResponse> mockResponse = List.of(
                new CouponV1Dto.UserCouponResponse(
                        100L, 10L, "10%할인", CouponType.RATE, new BigDecimal("10"), BigDecimal.ZERO, null, "AVAILABLE", LocalDateTime.now().plusDays(1)
                )
        );
        given(couponFacade.getUsersCoupons(userId)).willReturn(mockResponse);

        // when & then
        mockMvc.perform(get("/api/v1/users/me/coupons")
                        .header("X-Loopers-UserId", userId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].id").value(100L))
                .andExpect(jsonPath("$.data[0].name").value("10%할인"))
                .andExpect(jsonPath("$.data[0].status").value("AVAILABLE"));

        verify(couponFacade).getUsersCoupons(userId);
    }

    @Test
    @DisplayName("쿠폰 발급 요청 상태 조회 시 200 OK와 상태를 반환한다.")
    void getCouponRequestStatus_ShouldReturnOkAndStatus() throws Exception {
        // given
        String requestId = "test-req-123";
        given(couponFacade.getRequestStatus(requestId)).willReturn(com.loopers.domain.coupon.CouponRequestStatus.IN_PROGRESS);

        // when & then
        mockMvc.perform(get("/api/v1/coupons/requests/{requestId}", requestId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.requestId").value(requestId))
                .andExpect(jsonPath("$.data.status").value("IN_PROGRESS"));

        verify(couponFacade).getRequestStatus(requestId);
    }
}
