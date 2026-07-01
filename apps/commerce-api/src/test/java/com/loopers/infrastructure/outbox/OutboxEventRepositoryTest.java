package com.loopers.infrastructure.outbox;

import com.loopers.application.outbox.OutboxEventRepository;
import com.loopers.domain.outbox.EventType;
import com.loopers.domain.outbox.OutboxEvent;
import com.loopers.domain.outbox.OutboxEventStatus;
import com.loopers.utils.DatabaseCleanUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OutboxEventRepositoryTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private DatabaseCleanUp databaseCleanUp;

    @AfterEach
    void tearDown() {
        databaseCleanUp.truncateAllTables();
    }

    @Test
    @DisplayName("Outbox 이벤트를 저장하고 ID로 조회한다.")
    void saveAndFindById() {
        // given
        OutboxEvent event = new OutboxEvent(EventType.LIKE_CREATED, "{\"productId\":1,\"userId\":1}", OutboxEventStatus.INIT);

        // when
        OutboxEvent savedEvent = outboxEventRepository.save(event);

        // then
        assertThat(savedEvent.getId()).isNotNull();
        OutboxEvent foundEvent = outboxEventRepository.findById(savedEvent.getId()).orElseThrow();
        assertThat(foundEvent.getEventType()).isEqualTo(EventType.LIKE_CREATED);
        assertThat(foundEvent.getPayload()).isEqualTo("{\"productId\":1,\"userId\":1}");
        assertThat(foundEvent.getStatus()).isEqualTo(OutboxEventStatus.INIT);
    }

    @Test
    @DisplayName("특정 상태를 가진 Outbox 이벤트 목록을 조회한다.")
    void findAllByStatus() {
        // given
        OutboxEvent event1 = new OutboxEvent(EventType.LIKE_CREATED, "{}", OutboxEventStatus.INIT);
        OutboxEvent event2 = new OutboxEvent(EventType.LIKE_CREATED, "{}", OutboxEventStatus.COMPLETED);
        OutboxEvent event3 = new OutboxEvent(EventType.USER_ACTION_LOG, "{}", OutboxEventStatus.INIT);

        outboxEventRepository.save(event1);
        outboxEventRepository.save(event2);
        outboxEventRepository.save(event3);

        // when
        List<OutboxEvent> initEvents = outboxEventRepository.findAllByStatus(OutboxEventStatus.INIT);

        // then
        assertThat(initEvents).hasSize(2);
        assertThat(initEvents).extracting(OutboxEvent::getEventType)
                .containsExactlyInAnyOrder(EventType.LIKE_CREATED, EventType.USER_ACTION_LOG);
    }
}
