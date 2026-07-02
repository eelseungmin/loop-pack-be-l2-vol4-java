package com.loopers.application.payment;

import com.loopers.application.coupon.CouponRepository;
import com.loopers.application.product.ProductFacade;
import com.loopers.domain.order.OrderModel;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SyncPaymentCompensationHandler implements PaymentCompensationHandler {

    private final ProductFacade productFacade;
    private final CouponRepository couponRepository;

    @Override
    public void compensate(OrderModel order) {
        // 1. 재고 복구 (롤백)
        if (order.getItems() != null && !order.getItems().isEmpty()) {
            java.util.List<ProductFacade.StockRequest> stockRequests = order.getItems().stream()
                    .map(item -> new ProductFacade.StockRequest(item.getProductId(), item.getQuantity()))
                    .toList();
            productFacade.increaseStocks(stockRequests);
        }

        // 2. 쿠폰 복구 (롤백)
        if (order.getCouponIssueId() != null) {
            com.loopers.domain.coupon.CouponIssue couponIssue = couponRepository.findIssueById(order.getCouponIssueId())
                    .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "쿠폰 발급 내역을 찾을 수 없습니다."));
            couponIssue.restore();
            couponRepository.saveIssue(couponIssue);
        }
    }
}
