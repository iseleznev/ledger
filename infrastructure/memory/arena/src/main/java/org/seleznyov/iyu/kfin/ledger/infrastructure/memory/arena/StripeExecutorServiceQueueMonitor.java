package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import org.springframework.scheduling.annotation.Scheduled;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class StripeExecutorServiceQueueMonitor {

    private final ThreadPoolExecutor[] stripeExecutors;
    private final StripeMetrics stripeMetrics;
    private final StripeAlertingService alertingService;
    private final MeterRegistry meterRegistry;

    private static final int QUEUE_CAPACITY = 1000;
    private static final double HIGH_USAGE_THRESHOLD = 0.8;   // 80%
    private static final double CRITICAL_USAGE_THRESHOLD = 0.95; // 95%

    public StripeExecutorServiceQueueMonitor(
        ThreadPoolExecutor[] stripeExecutors,
        StripeMetrics stripeMetrics,
        StripeAlertingService alertingService,
        MeterRegistry meterRegistry
    ) {
        this.stripeExecutors = stripeExecutors;
        this.stripeMetrics = stripeMetrics;
        this.alertingService = alertingService;
        this.meterRegistry = meterRegistry;

        // Регистрируем Gauge'ы для каждого воркера
        registerGauges();
    }

    private void registerGauges() {
        for (int i = 0; i < stripeExecutors.length; i++) {
            final int workerIndex = i;
            final ThreadPoolExecutor executor = stripeExecutors[i];

            // Queue size
            Gauge.builder("stripe.queue.size")
                .tag("workerId", String.valueOf(workerIndex))
                .description("Current queue size")
                .register(meterRegistry, () -> executor.getQueue().size());

            // Queue usage percentage
            Gauge.builder("stripe.queue.usage.percent")
                .tag("workerId", String.valueOf(workerIndex))
                .description("Queue usage percentage")
                .register(meterRegistry, () -> {
                    return (double) executor.getQueue().size() / QUEUE_CAPACITY * 100;
                });

            // Active threads
            Gauge.builder("stripe.threads.active")
                .tag("workerId", String.valueOf(workerIndex))
                .description("Number of active threads")
                .register(meterRegistry, () -> executor.getActiveCount());

            // Completed tasks
            Gauge.builder("stripe.tasks.completed.total")
                .tag("workerId", String.valueOf(workerIndex))
                .description("Total completed tasks")
                .register(meterRegistry, () -> executor.getCompletedTaskCount());
        }
    }

    @Scheduled(fixedRate = 5000) // Каждые 5 секунд
    public void monitorQueues() {
        for (int i = 0; i < stripeExecutors.length; i++) {
            ThreadPoolExecutor executor = stripeExecutors[i];

            int queueSize = executor.getQueue().size();
            double usagePercent = (double) queueSize / QUEUE_CAPACITY;
            int activeCount = executor.getActiveCount();

            // Логирование состояния
            if (usagePercent > HIGH_USAGE_THRESHOLD) {
                log.warn("Stripe worker-{} high queue usage: {}/{} ({}%), active: {}",
                    i, queueSize, QUEUE_CAPACITY, String.format("%.1f", usagePercent * 100), activeCount);
            }

            // Алерты
            if (usagePercent > CRITICAL_USAGE_THRESHOLD) {
                log.error("Stripe worker-{} CRITICAL queue usage: {}/{} ({}%)",
                    i, queueSize, QUEUE_CAPACITY, String.format("%.1f", usagePercent * 100));

                alertingService.sendAlert(AlertLevel.CRITICAL,
                    "Stripe queue critical usage",
                    Map.of(
                        "workerId", i,
                        "queueSize", queueSize,
                        "capacity", QUEUE_CAPACITY,
                        "usagePercent", String.format("%.1f", usagePercent * 100),
                        "activeThreads", activeCount
                    ));
            } else if (usagePercent > HIGH_USAGE_THRESHOLD) {
                alertingService.sendAlert(AlertLevel.HIGH,
                    "Stripe queue high usage",
                    Map.of(
                        "workerId", i,
                        "queueSize", queueSize,
                        "usagePercent", String.format("%.1f", usagePercent * 100)
                    ));
            }
        }
    }

    // Дополнительный мониторинг здоровья воркеров
    @Scheduled(fixedRate = 30000) // Каждые 30 секунд
    public void healthCheck() {
        for (int i = 0; i < stripeExecutors.length; i++) {
            ThreadPoolExecutor executor = stripeExecutors[i];

            if (executor.isShutdown()) {
                log.error("Stripe worker-{} is SHUTDOWN!", i);
                alertingService.sendAlert(AlertLevel.CRITICAL,
                    "Stripe worker shutdown",
                    Map.of("workerId", i));
            }

            if (executor.isTerminated()) {
                log.error("Stripe worker-{} is TERMINATED!", i);
                alertingService.sendAlert(AlertLevel.CRITICAL,
                    "Stripe worker terminated",
                    Map.of("workerId", i));
            }
        }
    }
}