package com.loopers.application.inbox;

import com.loopers.domain.inbox.ConsumedMessage;

import java.util.Optional;

public interface ConsumedMessageRepository {
    ConsumedMessage save(ConsumedMessage consumedMessage);
    Optional<ConsumedMessage> findById(String messageId);
    boolean existsById(String messageId);
}
