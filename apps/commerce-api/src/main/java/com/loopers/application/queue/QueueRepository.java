package com.loopers.application.queue;

import java.util.List;
import java.util.Optional;

public interface QueueRepository {
    void enter(Long userId, long score);
    Optional<Long> getRank(Long userId);
    Long getWaitingCount();
    void makeActive(Long userId, String token, long ttlSeconds);
    Optional<String> getActiveToken(Long userId);
    void removeWaiting(Long userId);
    void removeActive(Long userId);
    List<Long> getFirstNWaiting(int n);
    void removeWaitingUsers(List<Long> userIds);
    QueueEntryResult enterAtomically(Long userId, long score);

    record QueueEntryResult(boolean isActive, String activeToken, Long rank, Long total) {}
}
