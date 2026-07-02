package com.loopers.infrastructure.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.loopers.application.coupon.CouponFacade;
import com.loopers.application.coupon.CouponRepository;
import com.loopers.application.coupon.CouponRequestRepository;
import com.loopers.domain.coupon.CouponRequestStatus;
import com.loopers.domain.coupon.CouponTemplate;
import com.loopers.domain.coupon.CouponType;
import com.loopers.testcontainers.RedisTestContainersConfig;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.ContextConfiguration;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class CouponKafkaConsumerTest {

    @Autowired
    private CouponKafkaConsumer couponKafkaConsumer;

    @Autowired
    private CouponRepository couponRepository;

    @Autowired
    private CouponRequestRepository couponRequestRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("발급 요청 이벤트를 받으면 비관적 락으로 발급을 성공시키고 Redis 상태를 SUCCESS로 변경한다.")
    void handleCouponIssueRequest_Success() throws Exception {
        // given
        Long userId = 1L;
        CouponTemplate template = couponRepository.saveTemplate(
                new CouponTemplate("선착순 10명 쿠폰", CouponType.FIXED, new BigDecimal("5000"), BigDecimal.ZERO, null, LocalDateTime.now().plusDays(10), 10, 0)
        );
        String requestId = "test-success-req-123";
        couponRequestRepository.saveStatus(requestId, CouponRequestStatus.IN_PROGRESS);

        com.loopers.application.coupon.CouponIssueRequestEvent event = new com.loopers.application.coupon.CouponIssueRequestEvent(requestId, userId, template.getId());
        String payload = objectMapper.writeValueAsString(event);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // when
        couponKafkaConsumer.handleCouponIssueRequest(payload, requestId, acknowledgment);

        // then
        // 1. Redis 상태가 SUCCESS 인지 확인
        CouponRequestStatus status = couponRequestRepository.findStatus(requestId).orElseThrow();
        assertThat(status).isEqualTo(CouponRequestStatus.SUCCESS);

        // 2. 실제로 DB 발급 내역이 저장되었는지 확인
        assertThat(couponRepository.findAllIssuesByUserId(userId)).hasSize(1);

        // 3. 수량 차감(issued_quantity가 1 증가)되었는지 확인
        CouponTemplate updated = couponRepository.findTemplateById(template.getId()).orElseThrow();
        assertThat(updated.getIssuedQuantity()).isEqualTo(1);

        // 4. acknowledgment가 호출되었는지 확인
        verify(acknowledgment).acknowledge();
    }

    @Test
    @DisplayName("수량이 모두 소진된 쿠폰 발급 이벤트를 처리하면 Redis 상태를 FAILED로 변경하고 ack()를 호출한다.")
    void handleCouponIssueRequest_Exhausted_ShouldSetFailedAndAck() throws Exception {
        // given
        Long userId = 2L;
        CouponTemplate template = couponRepository.saveTemplate(
                new CouponTemplate("선착순 1명 쿠폰", CouponType.FIXED, new BigDecimal("5000"), BigDecimal.ZERO, null, LocalDateTime.now().plusDays(10), 1, 1) // 이미 1개 발급 완료(소진)
        );
        String requestId = "test-fail-req-456";
        couponRequestRepository.saveStatus(requestId, CouponRequestStatus.IN_PROGRESS);

        com.loopers.application.coupon.CouponIssueRequestEvent event = new com.loopers.application.coupon.CouponIssueRequestEvent(requestId, userId, template.getId());
        String payload = objectMapper.writeValueAsString(event);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        // when
        couponKafkaConsumer.handleCouponIssueRequest(payload, requestId, acknowledgment);

        // then
        // 1. Redis 상태가 FAILED 인지 확인
        CouponRequestStatus status = couponRequestRepository.findStatus(requestId).orElseThrow();
        assertThat(status).isEqualTo(CouponRequestStatus.FAILED);

        // 2. DB 발급 내역이 생성되지 않았어야 함
        assertThat(couponRepository.findAllIssuesByUserId(userId)).isEmpty();

        // 3. acknowledgment가 호출되어야 무한 루프가 방지됨
        verify(acknowledgment).acknowledge();
    }
}
