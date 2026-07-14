package com.loopers.interfaces.api.queue;

import com.loopers.application.queue.QueueFacade;
import com.loopers.support.error.CoreException;
import com.loopers.support.error.ErrorType;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class TokenInterceptor implements HandlerInterceptor {

    @Autowired(required = false)
    private QueueFacade queueFacade;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (queueFacade == null) {
            return true;
        }
        String uri = request.getRequestURI();
        if ("POST".equalsIgnoreCase(request.getMethod()) && (uri.startsWith("/api/v1/orders") || uri.startsWith("/api/v1/payments"))) {
            String userIdStr = request.getHeader("X-Loopers-UserId");
            String token = request.getHeader("X-Queue-Token");

            if (userIdStr == null || userIdStr.isBlank()) {
                throw new CoreException(ErrorType.BAD_REQUEST, "X-Loopers-UserId 헤더가 누락되었습니다.");
            }

            Long userId;
            try {
                userId = Long.valueOf(userIdStr);
            } catch (NumberFormatException e) {
                throw new CoreException(ErrorType.BAD_REQUEST, "X-Loopers-UserId 헤더 값이 올바르지 않습니다.");
            }

            if (token == null || token.isBlank()) {
                throw new CoreException(ErrorType.QUEUE_TOKEN_INVALID);
            }

            boolean isValid = queueFacade.isValidActiveToken(userId, token);
            if (!isValid) {
                throw new CoreException(ErrorType.QUEUE_TOKEN_INVALID);
            }
        }
        return true;
    }
}
