package com.loopers.application.product;

import com.loopers.domain.brand.BrandModel;
import com.loopers.application.brand.BrandRepository;
import com.loopers.domain.product.ProductModel;
import com.loopers.application.product.ProductRepository;
import com.loopers.application.ranking.ProductRankingInfo;
import com.loopers.application.ranking.RankingRedisRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ProductFacadeTest {

    @InjectMocks
    private ProductFacade productFacade;

    @Mock
    private ProductRepository productRepository;

    @Mock
    private BrandRepository brandRepository;

    @Mock
    private org.springframework.data.redis.core.RedisTemplate<String, String> defaultRedisTemplate;

    @Mock
    private org.springframework.data.redis.core.ValueOperations<String, String> valueOperations;

    @Mock
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @Mock
    private com.loopers.domain.event.EventPublisher eventPublisher;

    @Mock
    private RankingRedisRepository rankingRedisRepository;

    @Test
    @DisplayName("상품 상세 조회 시 UserActionLogEvent(LOW)가 발행된다.")
    void getProduct_ShouldPublishUserActionLogEvent() {
        // given
        Long productId = 1L;
        BrandModel brand = new BrandModel("Nike");
        org.springframework.test.util.ReflectionTestUtils.setField(brand, "id", 10L);
        ProductModel product = new ProductModel(10L, "Air Max", new BigDecimal("1000.0000"));
        org.springframework.test.util.ReflectionTestUtils.setField(product, "id", productId);

        given(defaultRedisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get("product:detail::" + productId)).willReturn(null);
        given(productRepository.findById(productId)).willReturn(java.util.Optional.of(product));
        given(brandRepository.findById(10L)).willReturn(java.util.Optional.of(brand));

        // when
        productFacade.getProduct(productId);

        // then
        verify(eventPublisher).publish(org.mockito.ArgumentMatchers.any(com.loopers.domain.user.UserActionLogEvent.class));
    }

    @Test
    @DisplayName("상품 상세 조회 시 오늘 기준 랭킹 정보가 있으면 함께 반환한다.")
    void getProduct_WhenTodayRankingExists_ShouldReturnRankingInfo() {
        // given
        Long productId = 1L;
        BrandModel brand = new BrandModel("Nike");
        ReflectionTestUtils.setField(brand, "id", 10L);
        ProductModel product = new ProductModel(10L, "Air Max", new BigDecimal("1000.0000"));
        ReflectionTestUtils.setField(product, "id", productId);

        given(defaultRedisTemplate.opsForValue()).willReturn(valueOperations);
        given(valueOperations.get("product:detail::" + productId)).willReturn(null);
        given(productRepository.findById(productId)).willReturn(Optional.of(product));
        given(brandRepository.findById(10L)).willReturn(Optional.of(brand));
        given(rankingRedisRepository.findProductRanking(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.eq(productId)))
            .willReturn(Optional.of(new ProductRankingInfo("20260716", 3, 12.5)));

        // when
        ProductInfo productInfo = productFacade.getProduct(productId);

        // then
        assertThat(productInfo.ranking()).isNotNull();
        assertThat(productInfo.ranking().date()).isEqualTo("20260716");
        assertThat(productInfo.ranking().rank()).isEqualTo(3);
        assertThat(productInfo.ranking().score()).isEqualTo(12.5);
    }

    @Test
    @DisplayName("상품 목록을 페이지 조회하여 브랜드명을 병합한 DTO 목록을 반환한다.")
    void getProducts_ShouldReturnProductsWithBrandNames() {
        // given
        Long brandId = 10L;
        String sort = "latest";
        Pageable pageable = PageRequest.of(0, 10);

        ProductModel product1 = new ProductModel(brandId, "Air Max", new BigDecimal("1000.0000"));
        ReflectionTestUtils.setField(product1, "id", 1L);
        ReflectionTestUtils.setField(product1, "likeCount", 5);

        Page<ProductModel> productPage = new PageImpl<>(List.of(product1), pageable, 1);
        given(productRepository.findAll(brandId, sort, pageable)).willReturn(productPage);

        BrandModel brand = new BrandModel("Nike");
        ReflectionTestUtils.setField(brand, "id", brandId);
        given(brandRepository.findByIds(List.of(brandId))).willReturn(List.of(brand));

        // when
        Page<ProductInfo> result = productFacade.getProducts(brandId, sort, pageable);

        // then
        assertThat(result.getContent()).hasSize(1);
        ProductInfo info = result.getContent().get(0);
        assertThat(info.id()).isEqualTo(1L);
        assertThat(info.brandId()).isEqualTo(brandId);
        assertThat(info.brandName()).isEqualTo("Nike");
        assertThat(info.name()).isEqualTo("Air Max");
        assertThat(info.likeCount()).isEqualTo(5);
    }
}
