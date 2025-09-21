package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;

import javax.naming.ServiceUnavailableException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.Map;

@Slf4j
public class StripeQueueRejectionHandler implements RejectedExecutionHandler {

    private final int workerIndex;
    private final StripeAlertingService alertingService;
    private final StripeMetrics stripeMetrics;

    public StripeQueueRejectionHandler(
        int workerIndex,
        StripeAlertingService alertingService,
        StripeMetrics stripeMetrics
    ) {
        this.workerIndex = workerIndex;
        this.alertingService = alertingService;
        this.stripeMetrics = stripeMetrics;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // Метрики
        stripeMetrics.incrementQueueOverflow(workerIndex);

        log.warn("Stripe worker-{} queue overflow. Queue: {}/{}, Active: {}, Completed: {}",
            workerIndex,
            executor.getQueue().size(),
            getQueueCapacity(executor),
            executor.getActiveCount(),
            executor.getCompletedTaskCount()
        );

        // Алертинг при переполнении
        alertingService.sendAlert(
            AlertLevel.HIGH,
            "Stripe worker queue overflow",
            Map.of(
                "workerId", workerIndex,
                "queueSize", executor.getQueue().size(),
                "activeThreads", executor.getActiveCount(),
                "strategy", "CallerRuns"
            )
        );

        // CallerRuns стратегия - выполняем в текущем потоке
        if (!executor.isShutdown()) {
            try {
                log.info("Executing overflow task in caller thread for worker-{}", workerIndex);
                r.run();
                stripeMetrics.incrementCallerRunsExecutions(workerIndex);
            } catch (Exception e) {
                stripeMetrics.incrementCallerRunsFailures(workerIndex);
                log.error("Failed to execute overflow task in caller thread for worker-{}",
                    workerIndex, e);
                throw new RejectedExecutionException("Task rejected and failed in caller", e);
            }
        } else {
            // Executor shutdown - отклоняем
            stripeMetrics.incrementRejectedTasks(workerIndex);
            throw new ServiceUnavailableException(
                String.format("Stripe worker-%d is shutdown", workerIndex));
        }
    }

    private int getQueueCapacity(ThreadPoolExecutor executor) {
        if (executor.getQueue() instanceof java.util.concurrent.ArrayBlockingQueue) {
            return ((java.util.concurrent.ArrayBlockingQueue<?>) executor.getQueue()).remainingCapacity()
                + executor.getQueue().size();
        }
        return -1; // Unknown capacity
    }
}