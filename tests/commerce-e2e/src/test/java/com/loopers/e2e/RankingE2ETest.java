package com.loopers.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.testcontainers.RedisContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class RankingE2ETest {

    private static final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    private static final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build();

    private static final int API_PORT = 18080;
    private static final int STREAMER_PORT = 18081;
    private static final String ADMIN_LDAP = "loopers.admin";
    private static final String RANKING_TOPIC = "product-events";

    private static MySQLContainer<?> mysql;
    private static RedisContainer redis;
    private static KafkaContainer kafka;
    private static Process apiProcess;
    private static Process streamerProcess;

    @BeforeAll
    static void setUp() throws Exception {
        mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withDatabaseName("loopers")
            .withUsername("loopers")
            .withPassword("loopers");
        redis = new RedisContainer(DockerImageName.parse("redis:7.2"));
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

        mysql.start();
        redis.start();
        kafka.start();
        createTopic(RANKING_TOPIC);

        apiProcess = startApplication("apps/commerce-api", API_PORT);
        streamerProcess = startApplication("apps/commerce-streamer", STREAMER_PORT);

        waitForHealth(API_PORT, apiProcess);
        waitForHealth(STREAMER_PORT, streamerProcess);
    }

    @AfterAll
    static void tearDown() {
        stop(apiProcess);
        stop(streamerProcess);

        if (kafka != null) {
            kafka.stop();
        }
        if (redis != null) {
            redis.stop();
        }
        if (mysql != null) {
            mysql.stop();
        }
    }

    @DisplayName("Kafka 랭킹 이벤트가 Redis에 반영되고 랭킹 API에서 상품 정보와 함께 조회된다")
    @Test
    void rankingEventIsConsumedAndServedByRankingApi() throws Exception {
        Long brandId = postForData("/api-admin/v1/brands", Map.of("name", "Nike")).asLong();
        Long productId = postForData(
            "/api-admin/v1/products",
            Map.of(
                "brandId", brandId,
                "name", "Air Max",
                "price", BigDecimal.valueOf(10_000),
                "initialStock", 10
            )
        ).asLong();

        publishRankingEvent(productId, BigDecimal.valueOf(10_000), 2, LocalDateTime.of(2026, 7, 14, 10, 0));

        JsonNode ranking = pollRanking(productId);

        assertThat(ranking.get("rank").asLong()).isEqualTo(1L);
        assertThat(ranking.get("productId").asLong()).isEqualTo(productId);
        assertThat(ranking.get("productName").asText()).isEqualTo("Air Max");
        assertThat(ranking.get("brandId").asLong()).isEqualTo(brandId);
        assertThat(ranking.get("brandName").asText()).isEqualTo("Nike");
    }

    @DisplayName("일자가 변경되어도 이전 날짜 랭킹을 조회할 수 있다")
    @Test
    void rankingApi_ShouldServePreviousDateRanking() throws Exception {
        Long brandId = postForData("/api-admin/v1/brands", Map.of("name", "Previous Brand")).asLong();
        Long productId = postForData(
            "/api-admin/v1/products",
            Map.of(
                "brandId", brandId,
                "name", "Previous Product",
                "price", BigDecimal.valueOf(10_000),
                "initialStock", 10
            )
        ).asLong();

        publishRankingEvent(
            "e2e-ranking-yesterday-event",
            "ORDER",
            productId,
            BigDecimal.valueOf(10_000),
            1,
            LocalDateTime.of(2026, 7, 13, 23, 59)
        );

        JsonNode ranking = pollRanking(productId, "20260713");

        assertThat(ranking.get("rank").asLong()).isEqualTo(1L);
        assertThat(ranking.get("productId").asLong()).isEqualTo(productId);
        assertThat(ranking.get("productName").asText()).isEqualTo("Previous Product");
    }

    @DisplayName("주문 1건의 가중치가 좋아요 3건보다 높게 랭킹 순서에 반영된다")
    @Test
    void rankingApi_ShouldOrderByWeightedScore() throws Exception {
        Long brandId = postForData("/api-admin/v1/brands", Map.of("name", "Weight Brand")).asLong();
        Long orderedProductId = postForData(
            "/api-admin/v1/products",
            Map.of(
                "brandId", brandId,
                "name", "Ordered Product",
                "price", BigDecimal.valueOf(10_000),
                "initialStock", 10
            )
        ).asLong();
        Long likedProductId = postForData(
            "/api-admin/v1/products",
            Map.of(
                "brandId", brandId,
                "name", "Liked Product",
                "price", BigDecimal.valueOf(1_000),
                "initialStock", 10
            )
        ).asLong();

        publishRankingEvent("e2e-ranking-order-weight", "ORDER", orderedProductId, BigDecimal.valueOf(10_000), 1, LocalDateTime.of(2026, 7, 15, 10, 0));
        publishRankingEvent("e2e-ranking-like-weight-1", "LIKE", likedProductId, BigDecimal.ZERO, 0, LocalDateTime.of(2026, 7, 15, 10, 1));
        publishRankingEvent("e2e-ranking-like-weight-2", "LIKE", likedProductId, BigDecimal.ZERO, 0, LocalDateTime.of(2026, 7, 15, 10, 2));
        publishRankingEvent("e2e-ranking-like-weight-3", "LIKE", likedProductId, BigDecimal.ZERO, 0, LocalDateTime.of(2026, 7, 15, 10, 3));

        JsonNode orderedProductRanking = pollRanking(orderedProductId, "20260715");
        JsonNode likedProductRanking = pollRanking(likedProductId, "20260715");

        assertThat(orderedProductRanking.get("rank").asLong())
            .isLessThan(likedProductRanking.get("rank").asLong());
        assertThat(orderedProductRanking.get("score").asDouble())
            .isGreaterThan(likedProductRanking.get("score").asDouble());
    }

    private static JsonNode postForData(String path, Map<String, ?> body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:" + API_PORT + path))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .header("X-Loopers-Ldap", ADMIN_LDAP)
            .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body)))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isBetween(200, 299);
        return objectMapper.readTree(response.body()).get("data");
    }

    private static void publishRankingEvent(Long productId, BigDecimal price, int amount, LocalDateTime occurredAt) throws Exception {
        publishRankingEvent("e2e-ranking-event-1", "ORDER", productId, price, amount, occurredAt);
    }

    private static void publishRankingEvent(
        String eventId,
        String rankingEventType,
        Long productId,
        BigDecimal price,
        int amount,
        LocalDateTime occurredAt
    ) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Map<String, Object> payload = Map.of(
            "eventId", eventId,
            "rankingEventType", rankingEventType,
            "productId", productId,
            "price", price,
            "amount", amount,
            "occurredAt", occurredAt.toString()
        );

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(RANKING_TOPIC, productId.toString(), objectMapper.writeValueAsString(payload))).get(5, TimeUnit.SECONDS);
        }
    }

    private static JsonNode pollRanking(Long productId) throws Exception {
        return pollRanking(productId, "20260714");
    }

    private static JsonNode pollRanking(Long productId, String dateKey) throws Exception {
        Exception lastError = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);

        while (System.nanoTime() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + API_PORT + "/api/v1/rankings?date=" + dateKey + "&page=1&size=20"))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    JsonNode content = objectMapper.readTree(response.body()).path("data").path("content");
                    for (JsonNode item : content) {
                        if (item.path("productId").asLong() == productId) {
                            return item;
                        }
                    }
                }
            } catch (Exception exception) {
                lastError = exception;
            }

            Thread.sleep(500);
        }

        if (lastError != null) {
            throw lastError;
        }
        return fail("랭킹 API에서 상품을 찾지 못했습니다. productId=" + productId);
    }

    private static void createTopic(String topic) throws Exception {
        try (AdminClient adminClient = AdminClient.create(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ))) {
            adminClient.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(5, TimeUnit.SECONDS);
        }
    }

    private static Process startApplication(String projectPath, int port) throws IOException {
        Path jar = findBootJar(projectPath);
        ProcessBuilder builder = new ProcessBuilder(
            "java",
            "-jar",
            jar.toAbsolutePath().toString(),
            "--spring.profiles.active=test",
            "--server.port=" + port,
            "--management.server.port=" + port,
            "--spring.jpa.hibernate.ddl-auto=update",
            "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
            "--datasource.mysql-jpa.main.jdbc-url=" + mysql.getJdbcUrl(),
            "--datasource.mysql-jpa.main.username=" + mysql.getUsername(),
            "--datasource.mysql-jpa.main.password=" + mysql.getPassword(),
            "--datasource.redis.master.host=" + redis.getHost(),
            "--datasource.redis.master.port=" + redis.getRedisPort(),
            "--datasource.redis.replicas[0].host=" + redis.getHost(),
            "--datasource.redis.replicas[0].port=" + redis.getRedisPort()
        );
        builder.redirectErrorStream(true);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.environment().put("BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        return builder.start();
    }

    private static Path findBootJar(String projectPath) throws IOException {
        Path libs = Path.of(System.getProperty("e2e.root.dir")).resolve(projectPath).resolve("build").resolve("libs");
        try (Stream<Path> paths = Files.list(libs)) {
            return paths
                .filter(path -> path.getFileName().toString().endsWith(".jar"))
                .filter(path -> !path.getFileName().toString().endsWith("-plain.jar"))
                .sorted(Comparator.comparingLong(RankingE2ETest::lastModifiedTime).reversed())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("bootJar를 찾을 수 없습니다. path=" + libs));
        }
    }

    private static long lastModifiedTime(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException exception) {
            throw new IllegalStateException("jar 수정 시간을 읽을 수 없습니다. path=" + path, exception);
        }
    }

    private static void waitForHealth(int port, Process process) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);

        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                fail("애플리케이션 프로세스가 종료되었습니다. port=" + port);
            }

            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://localhost:" + port + "/actuator/health"))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() == 200) {
                    return;
                }
            } catch (Exception ignored) {
            }

            Thread.sleep(500);
        }

        fail("애플리케이션 기동 대기 시간이 초과되었습니다. port=" + port);
    }

    private static void stop(Process process) {
        if (process == null) {
            return;
        }

        process.destroy();
        try {
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }
}
