package com.loopers.interfaces.api.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.payment.PaymentFacade;
import com.loopers.domain.payment.PaymentMethod;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PaymentV1Controller.class)
class PaymentV1ControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private PaymentFacade paymentFacade;

    @Test
    @DisplayName("결제 요청(payments) 시 HTTP 200과 paymentId가 반환된다.")
    void processPayment_ApiSuccess() throws Exception {
        // given
        PaymentV1Dto.PaymentRequest request = new PaymentV1Dto.PaymentRequest(
                100L,
                PaymentMethod.CARD,
                new BigDecimal("50000")
        );

        given(paymentFacade.processPayment(eq(100L), eq(PaymentMethod.CARD), any(BigDecimal.class)))
                .willReturn(500L);

        // when & then
        mockMvc.perform(post("/api/v1/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.result").value("SUCCESS"))
                .andExpect(jsonPath("$.data.paymentId").value(500));
    }

    @Test
    @DisplayName("서킷 브레이커 OPEN 상태 시 결제 요청을 하면 503 SERVICE_UNAVAILABLE을 반환한다.")
    void processPayment_CircuitBreakerOpen_Returns503() throws Exception {
        // given
        PaymentV1Dto.PaymentRequest request = new PaymentV1Dto.PaymentRequest(
                100L,
                PaymentMethod.CARD,
                new BigDecimal("50000")
        );

        io.github.resilience4j.circuitbreaker.CircuitBreaker realCircuitBreaker = io.github.resilience4j.circuitbreaker.CircuitBreaker.ofDefaults("pgCircuitBreaker");

        io.github.resilience4j.circuitbreaker.CallNotPermittedException exception =
                io.github.resilience4j.circuitbreaker.CallNotPermittedException.createCallNotPermittedException(realCircuitBreaker);

        given(paymentFacade.processPayment(eq(100L), eq(PaymentMethod.CARD), any(BigDecimal.class)))
                .willThrow(exception);

        // when & then
        mockMvc.perform(post("/api/v1/payments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.meta.result").value("FAIL"))
                .andExpect(jsonPath("$.meta.errorCode").value("PAYMENT-503"))
                .andExpect(jsonPath("$.meta.message").value("현재 외부 결제 시스템 장애로 결제가 일시 중단되었습니다."));
    }

    @Test
    @DisplayName("PG 결제 콜백(callback) 수신 시 올바른 서명이면 HTTP 200 반환 및 Facade 호출 확인")
    void completePaymentCallback_ApiSuccess() throws Exception {
        // given
        PaymentV1Dto.PaymentCallbackRequest request = new PaymentV1Dto.PaymentCallbackRequest(
                500L,
                "DONE",
                "tx_abc_123"
        );

        // when & then
        mockMvc.perform(post("/api/v1/payments/callback")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-PG-Signature", "valid-signature-123")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.meta.result").value("SUCCESS"));

        // verify that facade's processCallback is called
        org.mockito.Mockito.verify(paymentFacade).processCallback(eq(500L), eq("tx_abc_123"), eq("DONE"), eq("valid-signature-123"));
    }

    @Test
    @DisplayName("위조된 PG 결제 콜백(잘못된 서명) 수신 시 UNAUTHORIZED 에러를 반환하고 처리하지 않는다.")
    void completePaymentCallback_InvalidSignature_Returns401() throws Exception {
        // given
        PaymentV1Dto.PaymentCallbackRequest request = new PaymentV1Dto.PaymentCallbackRequest(
                500L,
                "DONE",
                "tx_abc_123"
        );

        org.mockito.Mockito.doThrow(new com.loopers.support.error.CoreException(com.loopers.support.error.ErrorType.UNAUTHORIZED, "Invalid PG Signature"))
                .when(paymentFacade).processCallback(eq(500L), eq("tx_abc_123"), eq("DONE"), eq("invalid-fake-signature"));

        // when & then
        mockMvc.perform(post("/api/v1/payments/callback")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("X-PG-Signature", "invalid-fake-signature")
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.meta.result").value("FAIL"));
    }
}
