package com.loopers.domain.payment;

import java.math.BigDecimal;

public record PaymentFailedEvent(
        Long paymentId,
        Long orderId,
        Long userId,
        BigDecimal amount
) {}
