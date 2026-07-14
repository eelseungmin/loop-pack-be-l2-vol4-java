package com.loopers.interfaces.api.payment;

import com.loopers.application.payment.PaymentFacade;
import com.loopers.interfaces.api.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
public class PaymentV1Controller {

    private final PaymentFacade paymentFacade;

    @PostMapping
    public ApiResponse<PaymentV1Dto.PaymentResponse> processPayment(
            @RequestBody PaymentV1Dto.PaymentRequest request
    ) {
        Long paymentId = paymentFacade.processPayment(
                request.orderId(),
                request.paymentMethod(),
                request.amount()
        );
        return ApiResponse.success(new PaymentV1Dto.PaymentResponse(paymentId));
    }

    @PostMapping("/callback")
    public ApiResponse<Object> processCallback(
            @RequestBody PaymentV1Dto.PaymentCallbackRequest request,
            @org.springframework.web.bind.annotation.RequestHeader(value = "X-PG-Signature", required = false) String signature
    ) {
        paymentFacade.processCallback(request.paymentId(), request.transactionId(), request.status(), signature);
        return ApiResponse.success();
    }
}
