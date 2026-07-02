package com.loopers.infrastructure.inbox;

import com.loopers.application.inbox.ConsumedMessageRepository;
import com.loopers.domain.inbox.ConsumedMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class ConsumedMessageRepositoryImpl implements ConsumedMessageRepository {

    private final JpaConsumedMessageRepository jpaConsumedMessageRepository;

    @Override
    public ConsumedMessage save(ConsumedMessage consumedMessage) {
        return jpaConsumedMessageRepository.save(consumedMessage);
    }

    @Override
    public Optional<ConsumedMessage> findById(String messageId) {
        return jpaConsumedMessageRepository.findById(messageId);
    }

    @Override
    public boolean existsById(String messageId) {
        return jpaConsumedMessageRepository.existsById(messageId);
    }
}
