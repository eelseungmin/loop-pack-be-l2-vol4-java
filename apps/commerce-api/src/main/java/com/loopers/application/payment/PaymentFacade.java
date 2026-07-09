package com.loopers.application.payment;

import com.loopers.application.coupon.CouponRepository;
import com.loopers.application.order.OrderRepository;
import com.loopers.application.product.ProductFacade;
import com.loopers.application.queue.QueueRepository;
import com.loopers.domain.event.EventPublisher;
import com.loopers.domain.payment.PaymentMethod;
import com.loopers.domain.payment.PaymentModel;
import com.loopers.domain.payment.PaymentGateway;
import com.loopers.domain.payment.PaymentGateway.PaymentGatewayResult;
import com.loopers.domain.payment.PaymentStatus;
import com.loopers.domain.payment.PaymentCompletedEvent;
import com.loopers.domain.payment.PaymentFailedEvent;
import com.loopers.domain.user.UserActionLevel;
import com.loopers.domain.user.UserActionLogEvent;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentFacade {

    private static final List<PaymentStatus> BLOCKED_PAYMENT_STATUSES = List.of(PaymentStatus.READY, PaymentStatus.APPROVED);

    private final PaymentRepository paymentRepository;
    private final PaymentGateway paymentGateway;
    private final PaymentTempStorage paymentTempStorage;
    private final OrderRepository orderRepository;
    private final NotificationService notificationService;
    private final io.github.resilience4j.circuitbreaker.CircuitBreaker pgCircuitBreaker;
    private final EventPublisher eventPublisher;
    private final org.springframework.transaction.support.TransactionTemplate transactionTemplate;
    private final QueueRepository queueRepository;

    public PaymentStatus getPaymentStatus(Long paymentId) {
        return paymentRepository.findById(paymentId)
                .map(PaymentModel::getStatus)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));
    }

    public Long processPayment(Long orderId, PaymentMethod method, BigDecimal amount) {
        boolean locked = paymentTempStorage.lockOrder(orderId);
        if (!locked) {
            throw new CoreException(ErrorType.CONFLICT, "현재 주문에 대한 결제가 처리 중입니다.");
        }

        Long paymentId;
        try {
            // 0. 결제 상태 방어: 이미 READY 또는 APPROVED 상태인 결제가 존재하는 경우 409 CONFLICT 발생
            boolean existsPendingOrApproved = paymentRepository.existsByOrderIdAndStatusIn(orderId, BLOCKED_PAYMENT_STATUSES);
            if (existsPendingOrApproved) {
                throw new CoreException(ErrorType.CONFLICT, "이미 진행 중이거나 완료된 결제가 존재합니다.");
            }

            // 1. READY 상태로 저장 (단일 데이터 변경 작업, save API 자체 트랜잭션으로 바로 커밋)
            PaymentModel payment = paymentRepository.save(new PaymentModel(orderId, method, amount));
            paymentId = payment.getId();
        } finally {
            paymentTempStorage.unlockOrder(orderId);
        }

        // 2. Redis에 TTL 10초 설정 (추상화된 TempStorage 사용)
        try {
            paymentTempStorage.setRetryCount(paymentId, 0, Duration.ofSeconds(10));
        } catch (Exception e) {
            log.error("Failed to write to Redis for payment retry trace: {}", paymentId, e);
        }

        // 3. 외부 PG API 호출 (트랜잭션 바깥이므로 DB 락 및 커넥션 점유 없음)
        try {
            PaymentGatewayResult result = paymentGateway.requestPayment(orderId, amount, method);
            
            // 4. 성공 시 상태 APPROVED로 변경 (트랜잭션 묶음 처리)
            transactionTemplate.executeWithoutResult(status -> {
                PaymentModel targetPayment = paymentRepository.findById(paymentId)
                        .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));
                targetPayment.approve(result.transactionId(), result.approvedAt());
                paymentRepository.save(targetPayment);
                
                // 5. 성공 후 Redis 키 제거
                paymentTempStorage.deleteRetryKey(paymentId);

                // 6. 결제 완료 이벤트 발행 (아웃박스 저장을 위해 트랜잭션 내에서 발행)
                Long userId = orderRepository.findById(orderId)
                        .map(com.loopers.domain.order.OrderModel::getUserId)
                        .orElse(null);
                eventPublisher.publish(new PaymentCompletedEvent(paymentId, orderId, userId, amount));
                if (userId != null) {
                    try {
                        queueRepository.removeActive(userId);
                    } catch (Exception ex) {
                        log.error("Failed to remove active queue token for user: {}", userId, ex);
                    }
                }
            });
        } catch (Exception e) {
            log.warn("Payment PG request timeout or failed, keeping READY status for payment id: {}", paymentId, e);
        }

        return paymentId;
    }

    public void retryOrCompensatePayment(Long paymentId) {
        retryOrCompensatePayment(paymentId, false);
    }

    public void retryOrCompensatePayment(Long paymentId, boolean isFallback) {
        PaymentModel initial = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));

        if (initial.getStatus() != PaymentStatus.READY) {
            return;
        }
        Long orderId = initial.getOrderId();

        boolean locked = paymentTempStorage.lockOrder(orderId);
        if (!locked) {
            log.warn("Failed to acquire lock for payment: {}", paymentId);
            return;
        }

        try {
            PaymentModel check = paymentRepository.findById(paymentId)
                    .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));
            if (check.getStatus() != PaymentStatus.READY) {
                return;
            }

            PaymentGateway.PaymentGatewayQueryResult queryResult = paymentGateway.queryPaymentStatus(orderId);

            transactionTemplate.executeWithoutResult(status -> {
                executeRetryOrCompensateLogic(paymentId, isFallback, queryResult);
            });
        } finally {
            paymentTempStorage.unlockOrder(orderId);
        }
    }

    private void executeRetryOrCompensateLogic(Long paymentId, boolean isFallback, PaymentGateway.PaymentGatewayQueryResult queryResult) {
        PaymentModel payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));

        if (payment.getStatus() != PaymentStatus.READY) {
            return;
        }

        if (!isFallback && queryResult.status() == com.loopers.domain.payment.PaymentGatewayStatus.APPROVED) {
            payment.approve(queryResult.transactionId(), queryResult.approvedAt());
            paymentRepository.save(payment);

            com.loopers.domain.order.OrderModel order = orderRepository.findById(payment.getOrderId())
                    .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "주문 내역을 찾을 수 없습니다."));
            order.complete();
            orderRepository.save(order);

            paymentTempStorage.deleteRetryKey(paymentId);

            eventPublisher.publish(new PaymentCompletedEvent(paymentId, payment.getOrderId(), order.getUserId(), payment.getAmount()));
            try {
                queueRepository.removeActive(order.getUserId());
            } catch (Exception ex) {
                log.error("Failed to remove active queue token for user: {}", order.getUserId(), ex);
            }
        } else {
            Integer count = paymentTempStorage.getRetryCount(paymentId);
            if (count == null) {
                count = 0;
            }

            if (!isFallback && queryResult.status() == com.loopers.domain.payment.PaymentGatewayStatus.PENDING && count < 2) {
                paymentTempStorage.setRetryCount(paymentId, count + 1, Duration.ofSeconds(10));
            } else {
                com.loopers.domain.order.OrderModel order = orderRepository.findById(payment.getOrderId())
                        .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "주문 내역을 찾을 수 없습니다."));

                // Fallback 보정이면서 PG 결과가 APPROVED(결제성공)라면 즉시 결제 취소 API를 연동한다.
                if (isFallback && queryResult.status() == com.loopers.domain.payment.PaymentGatewayStatus.APPROVED) {
                    try {
                        log.info("Fallback correction: Canceling actual PG payment for payment: {}", paymentId);
                        paymentGateway.cancelPayment(queryResult.transactionId(), payment.getAmount());
                    } catch (Exception e) {
                        log.error("Failed to cancel PG payment for payment: {}", paymentId, e);
                        throw e;
                    }
                    try {
                        notificationService.sendPaymentRefund(order.getUserId(), paymentId);
                    } catch (Exception ne) {
                        log.error("Failed to send payment refund notification for user: {}, payment: {}", order.getUserId(), paymentId, ne);
                    }
                }

                payment.fail();
                paymentRepository.save(payment);

                order.cancel();
                orderRepository.save(order);

                // 보상 처리를 위한 비동기 이벤트(Outbox) 발행
                eventPublisher.publish(new PaymentFailedEvent(paymentId, payment.getOrderId(), order.getUserId(), payment.getAmount()));

                // 중요 비즈니스 실패 로깅 적용
                eventPublisher.publish(new UserActionLogEvent(
                        order.getUserId(),
                        "PAYMENT_FAILED",
                        String.format("{\"paymentId\":%d,\"orderId\":%d,\"amount\":%s}", paymentId, payment.getOrderId(), payment.getAmount()),
                        UserActionLevel.HIGH
                ));

                // 3. 알림 서비스 호출 (Fallback 스케줄러 보정이 아닐 때만 발송)
                if (!isFallback) {
                    try {
                        notificationService.sendPaymentTimeout(order.getUserId(), paymentId);
                    } catch (Exception e) {
                        log.error("Failed to send payment timeout notification for user: {}, payment: {}", order.getUserId(), paymentId, e);
                    }
                }

                paymentTempStorage.deleteRetryKey(paymentId);
            }
        }
    }

    public void completePayment(Long paymentId, String transactionId) {
        PaymentModel initial = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));

        if (initial.getStatus() != PaymentStatus.READY) {
            return;
        }
        Long orderId = initial.getOrderId();

        boolean locked = paymentTempStorage.lockOrder(orderId);
        if (!locked) {
            log.warn("Failed to acquire lock for payment: {}", paymentId);
            return;
        }

        try {
            transactionTemplate.executeWithoutResult(status -> {
                PaymentModel payment = paymentRepository.findById(paymentId)
                        .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "결제 내역을 찾을 수 없습니다."));

                if (payment.getStatus() != PaymentStatus.READY) {
                    return;
                }

                payment.approve(transactionId, java.time.LocalDateTime.now());
                paymentRepository.save(payment);

                com.loopers.domain.order.OrderModel order = orderRepository.findById(payment.getOrderId())
                        .orElseThrow(() -> new CoreException(ErrorType.NOT_FOUND, "주문 내역을 찾을 수 없습니다."));
                order.complete();
                orderRepository.save(order);

                paymentTempStorage.deleteRetryKey(paymentId);

                eventPublisher.publish(new PaymentCompletedEvent(paymentId, payment.getOrderId(), order.getUserId(), payment.getAmount()));
                try {
                    queueRepository.removeActive(order.getUserId());
                } catch (Exception ex) {
                    log.error("Failed to remove active queue token for user: {}", order.getUserId(), ex);
                }
            });
        } finally {
            paymentTempStorage.unlockOrder(orderId);
        }
    }
}
