package com.loopers.infrastructure.ranking;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;

public interface OutboxEventLogJpaRepository extends JpaRepository<OutboxEventLogJpaEntity, Long> {

    List<OutboxEventLogJpaEntity> findAllByEventTypeAndStatusInAndCreatedAtGreaterThanEqual(
        String eventType,
        Collection<String> statuses,
        LocalDateTime createdAt
    );
}
