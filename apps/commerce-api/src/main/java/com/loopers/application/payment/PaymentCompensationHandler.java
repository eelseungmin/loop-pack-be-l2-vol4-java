package com.loopers.application.payment;

import com.loopers.domain.order.OrderModel;

public interface PaymentCompensationHandler {
    void compensate(OrderModel order);
}
