package com.loopers.application.user;

import com.loopers.application.brand.BrandRepository;
import com.loopers.application.order.OrderCreateRequest;
import com.loopers.application.order.OrderFacade;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.application.product.ProductRepository;
import com.loopers.application.product.ProductFacade;
import com.loopers.application.payment.PaymentFacade;
import com.loopers.application.payment.PaymentRepository;
import com.loopers.application.order.OrderRepository;
import com.loopers.domain.brand.BrandModel;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.domain.payment.PaymentMethod;
import com.loopers.domain.payment.PaymentModel;
import com.loopers.domain.payment.PaymentGateway;
import com.loopers.domain.payment.PaymentGatewayStatus;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.data.redis.core.RedisTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class UserActionLoggingIntegrationTest {

    @Autowired
    private OrderFacade orderFacade;

    @Autowired
    private BrandRepository brandRepository;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private PaymentFacade paymentFacade;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private ProductFacade productFacade;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @SpyBean
    private PaymentGateway paymentGateway;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
        var keys = defaultRedisTemplate.keys("payment_retry:*");
        if (keys != null && !keys.isEmpty()) {
            defaultRedisTemplate.delete(keys);
        }
    }

    @Test
    @DisplayName("주문 생성을 요청하면 UserActionLogEvent(HIGH)가 발행되어 Outbox에 INIT 상태로 기록된다.")
    void createOrder_ShouldLogActionToOutbox() throws InterruptedException {
        // given
        Long userId = 1L;
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = new ProductModel(brand.getId(), "Air Max", new BigDecimal("100000"));
        product.assignStock(10);
        productRepository.save(product);

        OrderCreateRequest request = new OrderCreateRequest(
                List.of(new OrderCreateRequest.Item(product.getId(), 2)),
                null
        );

        // when
        Long orderId = orderFacade.createOrder(userId, request);

        // 비동기 처리 대기
        Thread.sleep(500);

        // then
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        assertThat(outboxEvents).isNotEmpty();
        OutboxEvent outboxEvent = outboxEvents.stream()
                .filter(e -> e.getEventType() == EventType.USER_ACTION_LOG)
                .findFirst()
                .orElseThrow();
        assertThat(outboxEvent.getPayload()).contains("CREATE_ORDER");
        assertThat(outboxEvent.getPayload()).contains(String.valueOf(orderId));
    }

    @Test
    @DisplayName("결제 실패(retry 초과) 시 UserActionLogEvent(HIGH)가 발행되어 Outbox에 INIT 상태로 기록된다.")
    void paymentFailed_ShouldLogActionToOutbox() throws InterruptedException {
        // given
        Long userId = 1L;
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = new ProductModel(brand.getId(), "Air Max", new BigDecimal("100000"));
        product.assignStock(10);
        productRepository.save(product);

        var order = new com.loopers.domain.order.OrderModel(userId, null, new BigDecimal("100000"), BigDecimal.ZERO, new BigDecimal("100000"));
        var savedOrder = orderRepository.save(order);

        var payment = new PaymentModel(savedOrder.getId(), PaymentMethod.CARD, new BigDecimal("100000"));
        var savedPayment = paymentRepository.save(payment);

        String redisKey = "payment_retry:" + savedPayment.getId();
        defaultRedisTemplate.opsForValue().set(redisKey, "2");

        Mockito.doReturn(new PaymentGateway.PaymentGatewayQueryResult(PaymentGatewayStatus.PENDING, null, null))
                .when(paymentGateway).queryPaymentStatus(savedOrder.getId());

        // when
        paymentFacade.retryOrCompensatePayment(savedPayment.getId());

        // 비동기 대기
        Thread.sleep(500);

        // then
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        OutboxEvent actionLog = outboxEvents.stream()
                .filter(e -> e.getEventType() == EventType.USER_ACTION_LOG)
                .findFirst()
                .orElseThrow();

        assertThat(actionLog.getPayload()).contains("PAYMENT_FAILED");
        assertThat(actionLog.getPayload()).contains(String.valueOf(savedPayment.getId()));
    }

    @Test
    @DisplayName("상품 단순 조회(VIEW_PRODUCT) 시 UserActionLogEvent(LOW)가 발행되지만 Outbox에는 기록되지 않는다.")
    void productView_ShouldLogLowLevelLogWithoutSavingToOutbox() throws InterruptedException {
        // given
        BrandModel brand = brandRepository.save(new BrandModel("Nike"));
        ProductModel product = productRepository.save(new ProductModel(brand.getId(), "Air Max", new BigDecimal("100000")));

        // when
        productFacade.getProduct(product.getId());

        // 비동기 대기
        Thread.sleep(500);

        // then
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);
        boolean hasLowLevelLog = outboxEvents.stream()
                .anyMatch(e -> e.getEventType() == EventType.USER_ACTION_LOG && e.getPayload().contains("VIEW_PRODUCT"));
        assertThat(hasLowLevelLog).isFalse();
    }
}
