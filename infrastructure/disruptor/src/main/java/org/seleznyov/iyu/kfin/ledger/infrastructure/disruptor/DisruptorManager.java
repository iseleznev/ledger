package org.seleznyov.iyu.kfin.ledger.infrastructure.disruptor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.event.LedgerEvent;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.LocalDate;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
@Slf4j
public class DisruptorManager {

    private static final int RING_BUFFER_SIZE = 8192; // Must be power of 2

    private final LedgerEventHandler eventHandler;

    private Disruptor<LedgerEvent> disruptor;
    private RingBuffer<LedgerEvent> ringBuffer;
    private final AtomicLong sequenceGenerator = new AtomicLong(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    // Track managed accounts (hot accounts currently in disruptor)
    private final Set<UUID> managedAccounts = ConcurrentHashMap.newKeySet();

    // Track pending operations for graceful shutdown
    private final ConcurrentHashMap<UUID, CompletableFuture<TransferResult>> pendingOperations =
        new ConcurrentHashMap<>();

    // Barrier management for snapshots
    private final ConcurrentHashMap<UUID, CompletableFuture<SnapshotBarrierResult>> pendingBarriers =
        new ConcurrentHashMap<>();
    private volatile SequenceBarrier sequenceBarrier;

    // Virtual thread executor for timeout handling
    private ExecutorService timeoutExecutor;

    // Snapshot barrier timeout
    private static final long BARRIER_TIMEOUT_MS = 5000; // 5 seconds

    @PostConstruct
    public void initialize() {
        log.info("Initializing LMAX Disruptor for ledger operations with virtual threads");

        // Create virtual thread executor for timeout handling
        timeoutExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Create disruptor with custom thread factory
        disruptor = new Disruptor<>(
            new LedgerEventFactory(),
            RING_BUFFER_SIZE,
            new VirtualThreadFactory(),
            ProducerType.SINGLE, // Single producer pattern
            new BlockingWaitStrategy() // Blocking strategy for consistency
        );

        // Register event handler
        disruptor.handleEventsWith(eventHandler);

        // Set exception handler
        disruptor.setDefaultExceptionHandler(new DisruptorExceptionHandler());

        // Start the disruptor
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
        sequenceBarrier = ringBuffer.newBarrier();

        initialized.set(true);

        log.info("LMAX Disruptor initialized successfully with buffer size: {}", RING_BUFFER_SIZE);
    }

    /**
     * Checks if disruptor is initialized and ready for processing.
     */
    public boolean isInitialized() {
        return initialized.get() && ringBuffer != null;
    }

    /**
     * Checks if specific account is currently managed by disruptor.
     */
    public boolean isAccountManaged(UUID accountId) {
        return managedAccounts.contains(accountId);
    }

    /**
     * Adds account to disruptor management (called when account becomes hot).
     */
    public void addManagedAccount(UUID accountId) {
        if (managedAccounts.add(accountId)) {
            log.debug("Added hot account to disruptor management: {}", accountId);
        }
    }

    /**
     * Removes account from disruptor management (called when account cools down).
     */
    public void removeManagedAccount(UUID accountId) {
        if (managedAccounts.remove(accountId)) {
            log.debug("Removed cooled account from disruptor management: {}", accountId);
        }
    }

    /**
     * Gets set of currently managed hot accounts.
     */
    public Set<UUID> getManagedAccounts() {
        return Set.copyOf(managedAccounts);
    }

    /**
     * Publishes transfer event to disruptor ring buffer.
     */
    public CompletableFuture<TransferResult> publishTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        if (!isInitialized()) {
            CompletableFuture<TransferResult> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                new IllegalStateException("DisruptorManager not initialized"));
            return failedFuture;
        }

        log.debug("Publishing transfer to disruptor: {} -> {}, amount: {}, tx: {}",
            debitAccountId, creditAccountId, amount, transactionId);

        CompletableFuture<TransferResult> resultFuture = new CompletableFuture<>();

        // Ensure both accounts are tracked as managed (hot accounts)
        addManagedAccount(debitAccountId);
        addManagedAccount(creditAccountId);

        try {
            // Get next sequence number
            long sequence = ringBuffer.next();

            try {
                // Get event from ring buffer
                LedgerEvent event = ringBuffer.get(sequence);

                // Initialize event data
                event.initializeTransfer(
                    debitAccountId, creditAccountId, amount, currencyCode,
                    operationDate, transactionId, idempotencyKey
                );

                // Set sequence number for tracking
                event.sequenceNumber(sequenceGenerator.incrementAndGet());

                // Track pending operation
                pendingOperations.put(transactionId, resultFuture);

                // Store result future in event handler for callback
                eventHandler.registerCallback(transactionId, resultFuture);

            } finally {
                // Publish event to ring buffer
                ringBuffer.publish(sequence);
            }

            log.debug("Transfer published to disruptor successfully: tx={}, sequence={}",
                transactionId, sequence);

        } catch (Exception e) {
            log.error("Failed to publish transfer to disruptor: tx={}, error={}",
                transactionId, e.getMessage());
            resultFuture.completeExceptionally(e);
        }

        return resultFuture;
    }

    /**
     * Publishes snapshot barrier event to ensure consistent snapshot creation.
     */
    public CompletableFuture<SnapshotBarrierResult> publishSnapshotBarrier(
        UUID accountId,
        LocalDate operationDate
    ) {
        if (!isInitialized()) {
            CompletableFuture<SnapshotBarrierResult> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(
                new IllegalStateException("DisruptorManager not initialized"));
            return failedFuture;
        }

        log.debug("Publishing snapshot barrier to disruptor: account={}, date={}",
            accountId, operationDate);

        CompletableFuture<SnapshotBarrierResult> resultFuture = new CompletableFuture<>();
        UUID barrierId = UUID.randomUUID();

        try {
            // Get next sequence for barrier event (don't wait for previous events here)
            long sequence = ringBuffer.next();

            try {
                // Get event from ring buffer
                LedgerEvent event = ringBuffer.get(sequence);

                // Initialize as snapshot trigger event
                event.clear();
                event.debitAccountId(accountId);
                event.operationDate(operationDate);
                event.transactionId(barrierId);
                event.eventType(LedgerEvent.EventType.SNAPSHOT_TRIGGER);
                event.sequenceNumber(sequenceGenerator.incrementAndGet());
                event.createdAt(java.time.LocalDateTime.now());
                event.status(LedgerEvent.EventStatus.PENDING);

                // Track pending barrier
                pendingBarriers.put(barrierId, resultFuture);

                // Register barrier callback with event handler
                eventHandler.registerBarrierCallback(barrierId, resultFuture);

            } finally {
                // Publish barrier event - the handler will wait for prior events
                ringBuffer.publish(sequence);
            }

            // Add timeout handling for barriers using virtual threads
            timeoutExecutor.submit(() -> {
                try {
                    Thread.sleep(BARRIER_TIMEOUT_MS);
                    if (!resultFuture.isDone()) {
                        pendingBarriers.remove(barrierId);
                        // Use custom SnapshotTimeoutException instead of disruptor's TimeoutException
                        resultFuture.completeExceptionally(
                            new SnapshotTimeoutException("Snapshot barrier timeout after " + BARRIER_TIMEOUT_MS + "ms")
                        );
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Timeout handler interrupted for barrier: {}", barrierId);
                }
            });

            log.debug("Snapshot barrier published: account={}, barrierId={}, sequence={}",
                accountId, barrierId, sequence);

        } catch (Exception e) {
            log.error("Failed to publish snapshot barrier: account={}, error={}",
                accountId, e.getMessage(), e);
            resultFuture.completeExceptionally(e);
        }

        return resultFuture;
    }

    /**
     * Completes pending transfer operation (called by event handler).
     */
    public void completeTransfer(UUID transactionId, TransferResult result) {
        CompletableFuture<TransferResult> future = pendingOperations.remove(transactionId);
        if (future != null) {
            future.complete(result);
            log.debug("Completed transfer: tx={}, success={}", transactionId, result.success());
        }
    }

    /**
     * Completes pending barrier operation (called by event handler).
     */
    public void completeBarrier(UUID barrierId, SnapshotBarrierResult result) {
        CompletableFuture<SnapshotBarrierResult> future = pendingBarriers.remove(barrierId);
        if (future != null) {
            future.complete(result);
            log.debug("Completed snapshot barrier: barrier={}, success={}", barrierId, result.success());
        }
    }

    /**
     * Gets current ring buffer utilization statistics.
     */
    public DisruptorStats getStats() {
        if (!isInitialized()) {
            return new DisruptorStats(0, 0, 0, 0, 0, 0);
        }

        long cursor = ringBuffer.getCursor();
        long remainingCapacity = ringBuffer.remainingCapacity();

        return new DisruptorStats(
            RING_BUFFER_SIZE,
            RING_BUFFER_SIZE - remainingCapacity, // used capacity
            pendingOperations.size(),
            pendingBarriers.size(),
            sequenceGenerator.get(),
            managedAccounts.size()
        );
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down LMAX Disruptor...");

        initialized.set(false);

        if (disruptor != null) {
            // Wait for pending operations to complete (max 30 seconds)
            long shutdownStart = System.currentTimeMillis();
            while ((!pendingOperations.isEmpty() || !pendingBarriers.isEmpty()) &&
                (System.currentTimeMillis() - shutdownStart) < 30000) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            int totalPending = pendingOperations.size() + pendingBarriers.size();
            if (totalPending > 0) {
                log.warn("Forcing shutdown with {} pending operations", totalPending);

                // Complete pending operations with failure
                pendingOperations.values().forEach(future ->
                    future.completeExceptionally(new RuntimeException("Disruptor shutdown")));
                pendingBarriers.values().forEach(future ->
                    future.completeExceptionally(new RuntimeException("Disruptor shutdown")));
            }

            disruptor.shutdown();
            managedAccounts.clear();

            // Shutdown timeout executor
            if (timeoutExecutor != null) {
                timeoutExecutor.shutdown();
                try {
                    if (!timeoutExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        timeoutExecutor.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    timeoutExecutor.shutdownNow();
                }
            }

            log.info("LMAX Disruptor shutdown completed");
        }
    }

    /**
     * Event factory for creating LedgerEvent instances.
     */
    private static class LedgerEventFactory implements EventFactory<LedgerEvent> {
        @Override
        public LedgerEvent newInstance() {
            return new LedgerEvent();
        }
    }

    /**
     * Virtual thread factory for disruptor operations.
     */
    private static class VirtualThreadFactory implements ThreadFactory {
        private final AtomicLong threadCounter = new AtomicLong(0);

        @Override
        public Thread newThread(Runnable r) {
            return Thread.ofVirtual()
                .name("ledger-disruptor-" + threadCounter.incrementAndGet())
                .unstarted(r);
        }
    }

    /**
     * Exception handler for disruptor events.
     */
    private static class DisruptorExceptionHandler implements com.lmax.disruptor.ExceptionHandler<LedgerEvent> {
        @Override
        public void handleEventException(Throwable ex, long sequence, LedgerEvent event) {
            log.error("Exception processing disruptor event at sequence {}: {}", sequence, ex.getMessage(), ex);

            // Mark event as failed
            if (event != null) {
                event.markFailed("Disruptor processing error: " + ex.getMessage());
            }
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            log.error("Exception starting disruptor: {}", ex.getMessage(), ex);
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            log.error("Exception shutting down disruptor: {}", ex.getMessage(), ex);
        }
    }

    /**
     * Custom timeout exception for snapshot barriers.
     */
    public static class SnapshotTimeoutException extends RuntimeException {
        public SnapshotTimeoutException(String message) {
            super(message);
        }
    }

    public record TransferResult(
        UUID transactionId,
        boolean success,
        String message,
        long processingTimeMs
    ) {}

    public record SnapshotBarrierResult(
        UUID accountId,
        boolean success,
        long balance,
        UUID lastEntryRecordId,
        long lastEntryOrdinal,
        int operationsCount,
        String errorMessage
    ) {}

    public record DisruptorStats(
        long bufferSize,
        long usedCapacity,
        long pendingOperations,
        long pendingBarriers,
        long totalEventsProcessed,
        long managedAccounts
    ) {}
}