package com.loopers.infrastructure.inbox;

import com.loopers.domain.inbox.ConsumedMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JpaConsumedMessageRepository extends JpaRepository<ConsumedMessage, String> {
}
