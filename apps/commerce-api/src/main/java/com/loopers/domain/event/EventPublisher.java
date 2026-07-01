package com.loopers.domain.event;

public interface EventPublisher {
    void publish(Object event);
}
