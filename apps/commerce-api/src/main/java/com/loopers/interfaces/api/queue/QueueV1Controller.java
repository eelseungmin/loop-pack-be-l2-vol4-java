package com.loopers.interfaces.api.queue;

import com.loopers.application.queue.QueueFacade;
import com.loopers.domain.queue.QueuePosition;
import com.loopers.interfaces.api.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping({"/queue", "/api/v1/queue"})
@RequiredArgsConstructor
public class QueueV1Controller {

    private final QueueFacade queueFacade;

    @PostMapping("/enter")
    public ApiResponse<QueueV1Dto.QueueEnterResponse> enterQueue(
            @RequestHeader("X-Loopers-UserId") Long userId
    ) {
        QueuePosition position = queueFacade.enterQueue(userId);
        return ApiResponse.success(new QueueV1Dto.QueueEnterResponse(
                position.status(),
                position.userId(),
                position.rank(),
                position.estimatedWaitTime(),
                position.pollingInterval()
        ));
    }

    @GetMapping("/position")
    public ApiResponse<QueueV1Dto.QueuePositionResponse> getQueuePosition(
            @RequestHeader("X-Loopers-UserId") Long userId
    ) {
        QueuePosition position = queueFacade.getQueuePosition(userId);
        return ApiResponse.success(new QueueV1Dto.QueuePositionResponse(
                position.status(),
                position.userId(),
                position.rank(),
                position.estimatedWaitTime(),
                position.pollingInterval(),
                position.token()
        ));
    }

    @GetMapping("/waiting-count")
    public ApiResponse<Long> getWaitingCount() {
        return ApiResponse.success(queueFacade.getWaitingCount());
    }
}
