package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.Map;

@Slf4j
public class StripeExceptionHandler {
    private final StripeMetrics stripeMetrics;
    private final StripeAlertingService alertingService;

    public StripeExceptionHandler(StripeMetrics stripeMetrics, StripeAlertingService alertingService) {
        this.stripeMetrics = stripeMetrics;
        this.alertingService = alertingService;
    }

    public Thread.UncaughtExceptionHandler createHandler(int workerIndex) {
        return (t, exception) -> {
            stripeMetrics.incrementThreadCrashes(workerIndex);

            log.error("Uncaught exception in stripe {} thread: {}", workerIndex, t.getName(), exception);

            // Анализ типа ошибки
            AlertLevel level = determineAlertLevel(exception);

            alertingService.sendAlert(level,
                "Stripe worker thread crashed",
                Map.of(
                    "workerId", workerIndex,
                    "threadName", t.getName(),
                    "exceptionType", exception.getClass().getSimpleName(),
                    "message", exception.getMessage(),
                    "isCritical", isCriticalException(exception)
                ));

            // Audit logging для финтеха
            auditLog("THREAD_CRASH", Map.of(
                "component", "stripe-worker",
                "workerId", workerIndex,
                "threadName", t.getName(),
                "exception", exception.getClass().getSimpleName(),
                "message", exception.getMessage()
            ));
        };
    }

    private AlertLevel determineAlertLevel(Throwable exception) {
        if (isCriticalException(exception)) {
            return AlertLevel.CRITICAL;
        } else if (isHighPriorityException(exception)) {
            return AlertLevel.HIGH;
        } else {
            return AlertLevel.MEDIUM;
        }
    }

    private boolean isCriticalException(Throwable exception) {
        return exception instanceof OutOfMemoryError ||
            exception instanceof StackOverflowError ||
            exception instanceof SQLException ||
            exception.getMessage().contains("deadlock") ||
            exception.getMessage().contains("timeout") ||
            exception.getMessage().contains("connection");
    }

    private boolean isHighPriorityException(Throwable exception) {
        return exception instanceof RuntimeException ||
            exception instanceof IllegalStateException ||
            exception.getMessage().contains("validation");
    }

    private void auditLog(String eventType, Map<String, Object> details) {
        // Ваша реализация audit logging
        log.info("AUDIT: {} - {}", eventType, details);
    }
}