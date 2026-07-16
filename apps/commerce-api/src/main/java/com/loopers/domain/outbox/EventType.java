package com.loopers.domain.outbox;

public enum EventType {
    LIKE_CREATED,
    LIKE_DELETED,
    USER_ACTION_LOG,
    PAYMENT_COMPLETED,
    PAYMENT_FAILED,
    PRODUCT_DELETED
}
