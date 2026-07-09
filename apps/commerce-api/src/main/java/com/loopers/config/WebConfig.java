package com.loopers.config;

import com.loopers.interfaces.api.queue.TokenInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Autowired(required = false)
    private TokenInterceptor tokenInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        if (tokenInterceptor != null) {
            registry.addInterceptor(tokenInterceptor)
                    .addPathPatterns("/api/v1/orders/**", "/api/v1/payments/**");
        }
    }
}
