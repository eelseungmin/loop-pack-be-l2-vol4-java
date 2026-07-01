package com.loopers.domain.user;

public record UserActionLogEvent(
        Long userId,
        String action,
        String payload,
        UserActionLevel level
) {}
