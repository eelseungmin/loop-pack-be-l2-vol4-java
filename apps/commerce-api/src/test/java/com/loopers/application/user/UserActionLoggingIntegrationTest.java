package com.loopers.application.user;

import com.loopers.application.brand.BrandRepository;
import com.loopers.application.order.OrderCreateRequest;
import com.loopers.application.order.OrderFacade;
import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.application.product.ProductRepository;
import com.loopers.domain.brand.BrandModel;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.domain.product.ProductModel;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;

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
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
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
}
