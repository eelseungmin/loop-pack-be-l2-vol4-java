package com.loopers.interfaces.api.queue;

import com.loopers.interfaces.api.ApiResponse;
import com.loopers.testcontainers.RedisTestContainersConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.*;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = RedisTestContainersConfig.class)
class QueueV1ApiE2ETest {

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private RedisTemplate<String, String> defaultRedisTemplate;

    @BeforeEach
    void setUp() {
        // Clear Redis before each test
        defaultRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    @DisplayName("대기열 진입 API (POST /queue/enter)")
    @Test
    void enterQueue_shouldAddUserToWaitingQueueAndReturnRank() {
        // arrange
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-UserId", "100");

        // act
        ResponseEntity<ApiResponse<QueueV1Dto.QueueEnterResponse>> response = testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // assert
        assertAll(
                () -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(response.getBody()).isNotNull(),
                () -> assertThat(response.getBody().data().status()).isEqualTo(com.loopers.domain.queue.QueueStatus.WAITING),
                () -> assertThat(response.getBody().data().userId()).isEqualTo(100L),
                () -> assertThat(response.getBody().data().rank()).isEqualTo(1L),
                () -> assertThat(response.getBody().data().estimatedWaitTime()).isEqualTo(0L),
                () -> assertThat(response.getBody().data().pollingInterval()).isEqualTo(1L)
        );
    }

    @DisplayName("대기열 중복 진입 방지 및 기존 순위 보장")
    @Test
    void enterQueue_shouldNotDuplicateAndPreserveRank() {
        // arrange
        HttpHeaders headers1 = new HttpHeaders();
        headers1.set("X-Loopers-UserId", "100");

        HttpHeaders headers2 = new HttpHeaders();
        headers2.set("X-Loopers-UserId", "200");

        // First user enters
        testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers1),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // Second user enters
        testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers2),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // act: First user re-enters
        ResponseEntity<ApiResponse<QueueV1Dto.QueueEnterResponse>> reEnterResponse = testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers1),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // assert
        assertAll(
                () -> assertThat(reEnterResponse.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(reEnterResponse.getBody()).isNotNull(),
                // Should still be rank 1 (not demoted or duplicated)
                () -> assertThat(reEnterResponse.getBody().data().rank()).isEqualTo(1L)
        );
    }

    @DisplayName("순번 조회 API (GET /queue/position) - WAITING 상태")
    @Test
    void getQueuePosition_whenWaiting_shouldReturnRankAndEstimatedTime() {
        // arrange
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-UserId", "100");

        // Enter queue
        testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // act
        ResponseEntity<ApiResponse<QueueV1Dto.QueuePositionResponse>> response = testRestTemplate.exchange(
                "/queue/position",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueuePositionResponse>>() {}
        );

        // assert
        assertAll(
                () -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(response.getBody()).isNotNull(),
                () -> assertThat(response.getBody().data().status()).isEqualTo(com.loopers.domain.queue.QueueStatus.WAITING),
                () -> assertThat(response.getBody().data().userId()).isEqualTo(100L),
                () -> assertThat(response.getBody().data().rank()).isEqualTo(1L),
                () -> assertThat(response.getBody().data().estimatedWaitTime()).isEqualTo(0L),
                // token should be null
                () -> assertThat(response.getBody().data().token()).isNull()
        );
    }

    @DisplayName("순번 조회 API (GET /queue/position) - ACTIVE 상태")
    @Test
    void getQueuePosition_whenActive_shouldReturnTokenAndNullRank() {
        // arrange
        Long userId = 100L;
        String token = "test-token-uuid-12345";
        // Manually make active in Redis
        defaultRedisTemplate.opsForValue().set("queue:active:" + userId, token);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-UserId", String.valueOf(userId));

        // act
        ResponseEntity<ApiResponse<QueueV1Dto.QueuePositionResponse>> response = testRestTemplate.exchange(
                "/queue/position",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueuePositionResponse>>() {}
        );

        // assert
        assertAll(
                () -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(response.getBody()).isNotNull(),
                () -> assertThat(response.getBody().data().status()).isEqualTo(com.loopers.domain.queue.QueueStatus.ACTIVE),
                () -> assertThat(response.getBody().data().userId()).isEqualTo(userId),
                () -> assertThat(response.getBody().data().rank()).isNull(),
                () -> assertThat(response.getBody().data().estimatedWaitTime()).isNull(),
                () -> assertThat(response.getBody().data().token()).isEqualTo(token)
        );
    }

    @DisplayName("대기자가 존재할 때 순번 조회 시 올바른 순번과 예상 대기 시간을 계산해 반환한다.")
    @Test
    void getQueuePosition_withWaitingUsers_shouldReturnCalculatedWaitTime() {
        // given
        for (long i = 1; i <= 42; i++) {
            defaultRedisTemplate.opsForZSet().add("queue:waiting", String.valueOf(i), (double) i);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-UserId", "100");

        testRestTemplate.exchange(
                "/queue/enter",
                HttpMethod.POST,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueueEnterResponse>>() {}
        );

        // when
        ResponseEntity<ApiResponse<QueueV1Dto.QueuePositionResponse>> response = testRestTemplate.exchange(
                "/queue/position",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueuePositionResponse>>() {}
        );

        // assert
        assertAll(
                () -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(response.getBody()).isNotNull(),
                () -> assertThat(response.getBody().data().status()).isEqualTo(com.loopers.domain.queue.QueueStatus.WAITING),
                () -> assertThat(response.getBody().data().rank()).isEqualTo(43L),
                () -> assertThat(response.getBody().data().estimatedWaitTime()).isEqualTo(2L)
        );
    }

    @DisplayName("순번 조회 시 대기 순번에 따른 동적 pollingInterval 반환 경계값을 검증한다.")
    @Test
    void getQueuePosition_pollingIntervalBoundary() {
        // given: 1001명의 유저를 대기열에 미리 삽입 (rank 1 ~ 1001)
        for (long i = 1; i <= 1001; i++) {
            defaultRedisTemplate.opsForZSet().add("queue:waiting", String.valueOf(i), (double) i);
        }

        // when & then: 각 순번에 해당하는 유저의 조회 결과 검증
        verifyPollingInterval("100", 100L, 1L);   // rank 100 -> 1L
        verifyPollingInterval("101", 101L, 3L);   // rank 101 -> 3L
        verifyPollingInterval("1000", 1000L, 3L); // rank 1000 -> 3L
        verifyPollingInterval("1001", 1001L, 5L); // rank 1001 -> 5L
    }

    private void verifyPollingInterval(String userId, Long expectedRank, Long expectedPollingInterval) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Loopers-UserId", userId);

        ResponseEntity<ApiResponse<QueueV1Dto.QueuePositionResponse>> response = testRestTemplate.exchange(
                "/queue/position",
                HttpMethod.GET,
                new HttpEntity<>(headers),
                new ParameterizedTypeReference<ApiResponse<QueueV1Dto.QueuePositionResponse>>() {}
        );

        assertAll(
                () -> assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK),
                () -> assertThat(response.getBody()).isNotNull(),
                () -> assertThat(response.getBody().data().rank()).isEqualTo(expectedRank),
                () -> assertThat(response.getBody().data().pollingInterval()).isEqualTo(expectedPollingInterval)
        );
    }
}
