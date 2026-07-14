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
        // 위조 콜백 방어 (서명 검증)
        if (signature == null || !isValidSignature(request, signature)) {
            throw new com.loopers.support.error.CoreException(com.loopers.support.error.ErrorType.UNAUTHORIZED, "Invalid PG Signature");
        }

        if ("DONE".equals(request.status())) {
            paymentFacade.completePayment(request.paymentId(), request.transactionId());
        }
        return ApiResponse.success();
    }

    private boolean isValidSignature(PaymentV1Dto.PaymentCallbackRequest request, String signature) {
        // 실제 운영에서는 PG사의 Secret Key를 이용한 Hash 검증 로직이 들어갑니다.
        return "valid-signature-123".equals(signature);
    }
}
