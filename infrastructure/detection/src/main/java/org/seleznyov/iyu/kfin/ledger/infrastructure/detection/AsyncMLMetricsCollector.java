package org.seleznyov.iyu.kfin.ledger.infrastructure.detection;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.detection.event.TransactionEvent;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
@RequiredArgsConstructor
@Slf4j
public class AsyncMLMetricsCollector {

    private final SlidingWindowMetrics slidingWindowMetrics;
    private final MLEnhancedHotAccountDetector hotAccountDetector;

    // Bounded queue для предотвращения memory leak
    private final BlockingQueue<TransactionEvent> eventQueue = new ArrayBlockingQueue<>(50_000);

    private ExecutorService processingExecutor;
    private volatile boolean running = false;

    // Метрики для мониторинга очереди
    private final AtomicLong eventsQueued = new AtomicLong(0);
    private final AtomicLong eventsProcessed = new AtomicLong(0);
    private final AtomicLong eventsDropped = new AtomicLong(0);

    @PostConstruct
    public void initialize() {
        processingExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "ml-metrics-processor");
            thread.setDaemon(true); // Не блокируем shutdown
            return thread;
        });

        running = true;
        processingExecutor.submit(this::processEvents);

        log.info("AsyncMLMetricsCollector initialized with queue capacity: {}", eventQueue.remainingCapacity());
    }

    /**
     * Неблокирующее добавление транзакции в очередь.
     * Вызывается из hot path операций.
     */
    public void recordTransactionAsync(UUID accountId, long amount, boolean isDebit) {
        if (!running) {
            return; // Skip if shutting down
        }

        TransactionEvent event = TransactionEvent.of(accountId, amount, isDebit);

        // offer() не блокирует - если очередь полная, event отбрасывается
        if (eventQueue.offer(event)) {
            eventsQueued.incrementAndGet();
        } else {
            eventsDropped.incrementAndGet();

            // Логируем переполнение, но не слишком часто
            if (eventsDropped.get() % 1000 == 0) {
                log.warn("ML metrics queue overflow, dropped {} events so far", eventsDropped.get());
            }
        }
    }

    private void processEvents() {
        log.info("Started ML metrics processing thread");

        while (running || !eventQueue.isEmpty()) {
            try {
                // Блокирующее получение события с timeout
                TransactionEvent event = eventQueue.poll(100, TimeUnit.MILLISECONDS);

                if (event != null) {
                    processEvent(event);
                    eventsProcessed.incrementAndGet();
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("ML metrics processing thread interrupted");
                break;
            } catch (Exception e) {
                log.error("Error processing ML metrics event: {}", e.getMessage(), e);
                // Continue processing other events
            }
        }

        log.info("ML metrics processing thread stopped. Processed: {}, Dropped: {}",
            eventsProcessed.get(), eventsDropped.get());
    }

    private void processEvent(TransactionEvent event) {
        try {
            // Обновляем sliding window metrics
            slidingWindowMetrics.recordTransaction(event.accountId(), event.amount(), event.isDebit());

            // Обновляем ML detector behavior profiles (сделать этот метод в detector)
            hotAccountDetector.updateBehaviorProfileAsync(event.accountId(), event.amount(), event.isDebit());

        } catch (Exception e) {
            log.error("Error updating ML metrics for account {}: {}", event.accountId(), e.getMessage());
        }
    }

    /**
     * Получить статистику очереди для мониторинга.
     */
    public AsyncMetricsStats getStats() {
        return new AsyncMetricsStats(
            eventQueue.size(),
            eventQueue.remainingCapacity(),
            eventsQueued.get(),
            eventsProcessed.get(),
            eventsDropped.get()
        );
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down AsyncMLMetricsCollector...");
        running = false;

        if (processingExecutor != null) {
            processingExecutor.shutdown();

            try {
                if (!processingExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warn("ML metrics processor didn't shutdown gracefully, forcing...");
                    processingExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                processingExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("AsyncMLMetricsCollector shutdown completed. Final stats: {}", getStats());
    }

    public record AsyncMetricsStats(
        int currentQueueSize,
        int remainingCapacity,
        long totalEventsQueued,
        long totalEventsProcessed,
        long totalEventsDropped
    ) {

    }
}