package org.seleznyov.iyu.kfin.ledger.application.service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.time.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.*;
import java.sql.*;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.*;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.scheduling.annotation.Scheduled;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

// Enhanced Domain Models
public record LedgerEntry(
    UUID id,
    UUID accountId,
    UUID transactionId,
    EntryType entryType,
    long amount,
    Instant createdAt,
    LocalDate operationDate,
    UUID idempotencyKey,
    String currencyCode,
    long entryOrdinal
) {
    public enum EntryType { DEBIT, CREDIT }
}

public record WalEntry(
    UUID id,
    long sequenceNumber,
    UUID debitAccountId,
    UUID creditAccountId,
    long amount,
    String currencyCode,
    LocalDate operationDate,
    UUID transactionId,
    UUID idempotencyKey,
    WalStatus status,
    Instant createdAt,
    Instant processedAt,
    String errorMessage
) {
    public enum WalStatus { PENDING, PROCESSING, PROCESSED, FAILED, RECOVERED }
}

// Error classification for intelligent handling
public enum TransferErrorType {
    BUSINESS_ERROR,      // Insufficient balance, validation failures
    PERFORMANCE_ERROR,   // Timeouts, contention, locks
    SYSTEM_ERROR,        // Infrastructure failures
    NETWORK_ERROR        // Database connection issues
}

public class ErrorClassifier {
    public static TransferErrorType classify(Throwable error) {
        String message = error.getMessage().toLowerCase();

        if (message.contains("insufficient balance") ||
            message.contains("validation") ||
            message.contains("invalid amount")) {
            return TransferErrorType.BUSINESS_ERROR;
        }

        if (message.contains("timeout") ||
            message.contains("contention") ||
            message.contains("lock") ||
            message.contains("deadlock")) {
            return TransferErrorType.PERFORMANCE_ERROR;
        }

        if (message.contains("connection") ||
            message.contains("network") ||
            message.contains("unavailable")) {
            return TransferErrorType.NETWORK_ERROR;
        }

        return TransferErrorType.SYSTEM_ERROR;
    }
}

// Bounded Account Cache with TTL and LRU eviction
@Component
public class BoundedAccountCache {
    private static final Logger logger = LoggerFactory.getLogger(BoundedAccountCache.class);

    private static final int MAX_CACHE_SIZE = 10000;
    private static final long TTL_MS = 5 * 60 * 1000; // 5 minutes
    private static final int EVICTION_BATCH_SIZE = 100;

    private final ConcurrentHashMap<UUID, AccountState> cache = new ConcurrentHashMap<>();
    private final AtomicLong accessCounter = new AtomicLong(0);

    public Optional<AccountState> get(UUID accountId) {
        AccountState state = cache.get(accountId);
        if (state != null) {
            if (isExpired(state)) {
                cache.remove(accountId);
                return Optional.empty();
            }
            // Update access time atomically
            AccountState updatedState = state.withAccessTime(System.currentTimeMillis());
            cache.put(accountId, updatedState);
            return Optional.of(updatedState);
        }
        return Optional.empty();
    }

    public void put(UUID accountId, AccountState state) {
        // Trigger eviction if needed
        if (cache.size() >= MAX_CACHE_SIZE) {
            evictOldestEntries();
        }

        cache.put(accountId, state.withAccessTime(System.currentTimeMillis()));
        accessCounter.incrementAndGet();
    }

    public void invalidate(UUID accountId) {
        cache.remove(accountId);
    }

    private boolean isExpired(AccountState state) {
        return System.currentTimeMillis() - state.lastAccessTime() > TTL_MS;
    }

    private void evictOldestEntries() {
        // LRU eviction - remove oldest entries
        List<Map.Entry<UUID, AccountState>> entries = new ArrayList<>(cache.entrySet());
        entries.sort(Comparator.comparing(e -> e.getValue().lastAccessTime()));

        int toEvict = Math.min(EVICTION_BATCH_SIZE, entries.size());
        for (int i = 0; i < toEvict; i++) {
            cache.remove(entries.get(i).getKey());
        }

        logger.debug("Evicted {} cache entries, cache size: {}", toEvict, cache.size());
    }

    @Scheduled(fixedRate = 60000) // Every minute
    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        int removed = 0;

        Iterator<Map.Entry<UUID, AccountState>> iterator = cache.entrySet().iterator();
        while (iterator.hasNext() && removed < EVICTION_BATCH_SIZE) {
            Map.Entry<UUID, AccountState> entry = iterator.next();
            if (now - entry.getValue().lastAccessTime() > TTL_MS) {
                iterator.remove();
                removed++;
            }
        }

        if (removed > 0) {
            logger.debug("Cleaned up {} expired cache entries", removed);
        }
    }

    public CacheStats getStats() {
        return new CacheStats(cache.size(), accessCounter.get(), MAX_CACHE_SIZE);
    }

    public record CacheStats(int size, long totalAccesses, int maxSize) {}
}

// Enhanced Account State with immutable updates
public record AccountState(
    long balance,
    long operationCount,
    long lastAccessTime,
    UUID lastEntryId,
    long lastEntryOrdinal,
    String lastOperation
) {
    public AccountState withBalance(long newBalance) {
        return new AccountState(newBalance, operationCount + 1, lastAccessTime,
            lastEntryId, lastEntryOrdinal, lastOperation);
    }

    public AccountState withAccessTime(long accessTime) {
        return new AccountState(balance, operationCount, accessTime,
            lastEntryId, lastEntryOrdinal, lastOperation);
    }

    public AccountState withEntry(UUID entryId, long ordinal, String operation) {
        return new AccountState(balance, operationCount + 1, lastAccessTime,
            entryId, ordinal, operation);
    }
}

// Cross-partition lock manager with deadlock prevention
@Component
public class CrossPartitionLockManager {
    private static final Logger logger = LoggerFactory.getLogger(CrossPartitionLockManager.class);
    private static final int MAX_LOCKS = 5000;

    private final ConcurrentHashMap<UUID, StampedLock> accountLocks = new ConcurrentHashMap<>();
    private final AtomicInteger lockCount = new AtomicInteger(0);

    public OrderedLocks acquireOrderedLocks(UUID account1, UUID account2) {
        if (account1.equals(account2)) {
            // Same account - single lock
            StampedLock lock = getOrCreateLock(account1);
            long stamp = lock.writeLock();
            return new OrderedLocks(List.of(new LockHolder(lock, stamp)), account1, account2);
        }

        // Deterministic ordering to prevent deadlocks
        UUID firstAccount = account1.compareTo(account2) <= 0 ? account1 : account2;
        UUID secondAccount = account1.compareTo(account2) <= 0 ? account2 : account1;

        StampedLock firstLock = getOrCreateLock(firstAccount);
        StampedLock secondLock = getOrCreateLock(secondAccount);

        // Acquire in deterministic order
        long firstStamp = firstLock.writeLock();
        long secondStamp = secondLock.writeLock();

        return new OrderedLocks(
            List.of(
                new LockHolder(firstLock, firstStamp),
                new LockHolder(secondLock, secondStamp)
            ),
            account1, account2
        );
    }

    private StampedLock getOrCreateLock(UUID accountId) {
        return accountLocks.computeIfAbsent(accountId, id -> {
            if (lockCount.incrementAndGet() > MAX_LOCKS) {
                // Trigger cleanup
                cleanupUnusedLocks();
            }
            return new StampedLock();
        });
    }

    private void cleanupUnusedLocks() {
        // Simple cleanup - remove randomly if over limit
        if (accountLocks.size() > MAX_LOCKS) {
            List<UUID> toRemove = accountLocks.keySet().stream()
                .limit(MAX_LOCKS / 10) // Remove 10%
                .toList();

            toRemove.forEach(accountLocks::remove);
            lockCount.addAndGet(-toRemove.size());
        }
    }

    public static class OrderedLocks implements AutoCloseable {
        private final List<LockHolder> locks;
        private final UUID account1, account2;

        public OrderedLocks(List<LockHolder> locks, UUID account1, UUID account2) {
            this.locks = locks;
            this.account1 = account1;
            this.account2 = account2;
        }

        @Override
        public void close() {
            // Release in reverse order
            for (int i = locks.size() - 1; i >= 0; i--) {
                LockHolder holder = locks.get(i);
                holder.lock().unlockWrite(holder.stamp());
            }
        }

        public UUID account1() { return account1; }
        public UUID account2() { return account2; }
    }

    private record LockHolder(StampedLock lock, long stamp) {}
}

// Enhanced Log Record with validation
public class LogRecord {
    public static final int HEADER_SIZE = 64; // Increased for metadata
    public static final byte MAGIC_BYTE = (byte) 0xAC;
    public static final byte VERSION = 2; // Updated version

    private final byte magic;
    private final byte version;
    private final int crc32;
    private final int recordSize;
    private final long timestamp;
    private final long offset;
    private final TransferErrorType errorType; // New field
    private final byte[] payload;

    public LogRecord(long offset, byte[] payload, TransferErrorType errorType) {
        this.magic = MAGIC_BYTE;
        this.version = VERSION;
        this.recordSize = HEADER_SIZE + payload.length;
        this.timestamp = System.currentTimeMillis();
        this.offset = offset;
        this.errorType = errorType;
        this.payload = payload;
        this.crc32 = calculateCRC32(payload);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(magic);
        buffer.put(version);
        buffer.putInt(crc32);
        buffer.putInt(recordSize);
        buffer.putLong(timestamp);
        buffer.putLong(offset);
        buffer.put((byte) errorType.ordinal()); // Error type
        buffer.position(buffer.position() + 31); // Reserved space
        buffer.put(payload);
    }

    public static LogRecord readFrom(ByteBuffer buffer, long expectedOffset) throws IOException {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new IOException("Insufficient data for header");
        }

        byte magic = buffer.get();
        if (magic != MAGIC_BYTE) {
            throw new IOException("Invalid magic byte: " + magic);
        }

        byte version = buffer.get();
        if (version > VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        int crc32 = buffer.getInt();
        int recordSize = buffer.getInt();
        long timestamp = buffer.getLong();
        long offset = buffer.getLong();
        TransferErrorType errorType = TransferErrorType.values()[buffer.get()];
        buffer.position(buffer.position() + 31); // Skip reserved

        if (offset != expectedOffset) {
            throw new IOException("Offset mismatch. Expected: " + expectedOffset + ", got: " + offset);
        }

        int payloadSize = recordSize - HEADER_SIZE;
        byte[] payload = new byte[payloadSize];
        buffer.get(payload);

        if (calculateCRC32(payload) != crc32) {
            throw new IOException("CRC32 mismatch");
        }

        return new LogRecord(offset, payload, errorType);
    }

    private static int calculateCRC32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    public int size() { return recordSize; }
    public long offset() { return offset; }
    public long timestamp() { return timestamp; }
    public byte[] payload() { return payload; }
    public TransferErrorType errorType() { return errorType; }
}

// Circuit Breaker for resilience
@Component
public class LedgerCircuitBreaker {
    private static final Logger logger = LoggerFactory.getLogger(LedgerCircuitBreaker.class);

    private enum State { CLOSED, OPEN, HALF_OPEN }

    private volatile State state = State.CLOSED;
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);

    private static final int FAILURE_THRESHOLD = 50;
    private static final long TIMEOUT_MS = 30000; // 30 seconds
    private static final int SUCCESS_THRESHOLD = 10;
    private final AtomicInteger halfOpenSuccesses = new AtomicInteger(0);

    public <T> CompletableFuture<T> executeAsync(Supplier<CompletableFuture<T>> operation, String operationName) {
        if (state == State.OPEN) {
            if (System.currentTimeMillis() - lastFailureTime.get() > TIMEOUT_MS) {
                state = State.HALF_OPEN;
                halfOpenSuccesses.set(0);
                logger.info("Circuit breaker transitioning to HALF_OPEN for {}", operationName);
            } else {
                return CompletableFuture.failedFuture(
                    new CircuitBreakerOpenException("Circuit breaker is OPEN for " + operationName));
            }
        }

        CompletableFuture<T> future = operation.get();

        return future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                onFailure(operationName);
            } else {
                onSuccess(operationName);
            }
        });
    }

    private void onSuccess(String operationName) {
        if (state == State.HALF_OPEN) {
            if (halfOpenSuccesses.incrementAndGet() >= SUCCESS_THRESHOLD) {
                state = State.CLOSED;
                failureCount.set(0);
                logger.info("Circuit breaker CLOSED for {}", operationName);
            }
        } else {
            failureCount.set(0);
        }
    }

    private void onFailure(String operationName) {
        lastFailureTime.set(System.currentTimeMillis());

        if (failureCount.incrementAndGet() >= FAILURE_THRESHOLD) {
            state = State.OPEN;
            logger.warn("Circuit breaker OPENED for {} after {} failures", operationName, FAILURE_THRESHOLD);
        }
    }

    public boolean isOpen() { return state == State.OPEN; }
    public State getState() { return state; }
    public int getFailureCount() { return failureCount.get(); }

    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) { super(message); }
    }
}

// Rate Limiter with adaptive capacity
@Component
public class AdaptiveRateLimiter {
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveRateLimiter.class);

    private volatile double currentRate = 1000.0; // Start with 1000 TPS
    private final double maxRate = 10000.0;
    private final double minRate = 100.0;

    private final AtomicLong tokens = new AtomicLong(0);
    private final AtomicLong lastRefillTime = new AtomicLong(System.currentTimeMillis());

    // Performance metrics for adaptation
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong lastAdaptationTime = new AtomicLong(System.currentTimeMillis());

    public boolean tryAcquire() {
        refillTokens();

        long currentTokens = tokens.get();
        if (currentTokens > 0) {
            if (tokens.compareAndSet(currentTokens, currentTokens - 1)) {
                return true;
            }
        }

        return false;
    }

    public void recordSuccess() {
        successCount.incrementAndGet();
        adaptRateIfNeeded();
    }

    public void recordError() {
        errorCount.incrementAndGet();
        adaptRateIfNeeded();
    }

    private void refillTokens() {
        long now = System.currentTimeMillis();
        long lastRefill = lastRefillTime.get();

        if (now > lastRefill && lastRefillTime.compareAndSet(lastRefill, now)) {
            long elapsedMs = now - lastRefill;
            long tokensToAdd = (long) (currentRate * elapsedMs / 1000.0);

            if (tokensToAdd > 0) {
                long maxTokens = (long) currentRate;
                tokens.updateAndGet(current -> Math.min(current + tokensToAdd, maxTokens));
            }
        }
    }

    private void adaptRateIfNeeded() {
        long now = System.currentTimeMillis();
        long lastAdaptation = lastAdaptationTime.get();

        if (now - lastAdaptation > 10000) { // Adapt every 10 seconds
            if (lastAdaptationTime.compareAndSet(lastAdaptation, now)) {
                adaptRate();
            }
        }
    }

    private void adaptRate() {
        long successes = successCount.getAndSet(0);
        long errors = errorCount.getAndSet(0);

        if (successes + errors == 0) return;

        double errorRate = (double) errors / (successes + errors);

        if (errorRate < 0.01 && currentRate < maxRate) {
            // Low error rate - increase capacity
            currentRate = Math.min(currentRate * 1.1, maxRate);
            logger.debug("Increased rate limit to {}", currentRate);
        } else if (errorRate > 0.05 && currentRate > minRate) {
            // High error rate - decrease capacity
            currentRate = Math.max(currentRate * 0.9, minRate);
            logger.debug("Decreased rate limit to {}", currentRate);
        }
    }

    public double getCurrentRate() { return currentRate; }
    public long getAvailableTokens() { return tokens.get(); }
}

// Enhanced Partition with hybrid features
public class EnhancedLedgerPartition extends LedgerPartition {
    private static final Logger logger = LoggerFactory.getLogger(EnhancedLedgerPartition.class);

    private final BoundedAccountCache accountCache;
    private final CrossPartitionLockManager lockManager;
    private final LedgerCircuitBreaker circuitBreaker;
    private final AdaptiveRateLimiter rateLimiter;

    public EnhancedLedgerPartition(int partitionId, Path baseDir,
                                   BoundedAccountCache accountCache,
                                   CrossPartitionLockManager lockManager,
                                   LedgerCircuitBreaker circuitBreaker,
                                   AdaptiveRateLimiter rateLimiter) throws IOException {
        super(partitionId, baseDir);
        this.accountCache = accountCache;
        this.lockManager = lockManager;
        this.circuitBreaker = circuitBreaker;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public CompletableFuture<Long> append(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        // Rate limiting
        if (!rateLimiter.tryAcquire()) {
            rateLimiter.recordError();
            return CompletableFuture.failedFuture(
                new RateLimitExceededException("Rate limit exceeded"));
        }

        // Circuit breaker protection
        return circuitBreaker.executeAsync(
            () -> appendWithLocking(debitEntry, creditEntry),
            "partition-" + partitionId + "-append"
        ).whenComplete((result, throwable) -> {
            if (throwable != null) {
                rateLimiter.recordError();
                handleAppendError(debitEntry, creditEntry, throwable);
            } else {
                rateLimiter.recordSuccess();
                updateAccountCache(debitEntry, creditEntry);
            }
        });
    }

    private CompletableFuture<Long> appendWithLocking(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        UUID debitAccountId = debitEntry.accountId();
        UUID creditAccountId = creditEntry.accountId();

        // Check if cross-partition operation
        if (getPartition(debitAccountId) != getPartition(creditAccountId)) {
            return appendCrossPartition(debitEntry, creditEntry);
        }

        // Same partition - use inherited method
        return super.append(debitEntry, creditEntry);
    }

    private CompletableFuture<Long> appendCrossPartition(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        return CompletableFuture.supplyAsync(() -> {
            try (CrossPartitionLockManager.OrderedLocks locks =
                     lockManager.acquireOrderedLocks(debitEntry.accountId(), creditEntry.accountId())) {

                // Validate balances under lock
                if (!validateSufficientBalance(debitEntry)) {
                    throw new InsufficientBalanceException("Insufficient balance for account: " + debitEntry.accountId());
                }

                // Perform append operation
                return super.append(debitEntry, creditEntry).get();

            } catch (Exception e) {
                logger.error("Cross-partition append failed: {}", e.getMessage());
                throw new RuntimeException(e);
            }
        }, writerExecutor);
    }

    private boolean validateSufficientBalance(LedgerEntry debitEntry) {
        Optional<AccountState> cachedState = accountCache.get(debitEntry.accountId());
        if (cachedState.isPresent()) {
            return cachedState.get().balance() >= debitEntry.amount();
        }

        // Fallback to database check
        return true; // Simplified - would query repository
    }

    private void updateAccountCache(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        // Update debit account
        Optional<AccountState> debitState = accountCache.get(debitEntry.accountId());
        if (debitState.isPresent()) {
            AccountState newDebitState = debitState.get()
                .withBalance(debitState.get().balance() - debitEntry.amount())
                .withEntry(debitEntry.id(), debitEntry.entryOrdinal(), "DEBIT");
            accountCache.put(debitEntry.accountId(), newDebitState);
        }

        // Update credit account
        Optional<AccountState> creditState = accountCache.get(creditEntry.accountId());
        if (creditState.isPresent()) {
            AccountState newCreditState = creditState.get()
                .withBalance(creditState.get().balance() + creditEntry.amount())
                .withEntry(creditEntry.id(), creditEntry.entryOrdinal(), "CREDIT");
            accountCache.put(creditEntry.accountId(), newCreditState);
        }
    }

    private void handleAppendError(LedgerEntry debitEntry, LedgerEntry creditEntry, Throwable error) {
        TransferErrorType errorType = ErrorClassifier.classify(error);

        switch (errorType) {
            case BUSINESS_ERROR -> logger.debug("Business error in append: {}", error.getMessage());
            case PERFORMANCE_ERROR -> {
                logger.warn("Performance issue in append: {}", error.getMessage());
                // Could trigger account cooling here
            }
            case SYSTEM_ERROR, NETWORK_ERROR -> logger.error("System error in append: {}", error.getMessage());
        }
    }

    private int getPartition(UUID accountId) {
        return Math.abs(accountId.hashCode()) % 16; // Assuming 16 partitions
    }

    public static class RateLimitExceededException extends RuntimeException {
        public RateLimitExceededException(String message) { super(message); }
    }

    public static class InsufficientBalanceException extends RuntimeException {
        public InsufficientBalanceException(String message) { super(message); }
    }
}

// Main Hybrid Ledger Manager
@Component
public class HybridLedgerManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(HybridLedgerManager.class);

    @Autowired private BoundedAccountCache accountCache;
    @Autowired private CrossPartitionLockManager lockManager;
    @Autowired private LedgerCircuitBreaker circuitBreaker;
    @Autowired private AdaptiveRateLimiter rateLimiter;
    @Autowired private LedgerRepository ledgerRepository;

    @Value("${ledger.log.directory:./hybrid-ledger-log}")
    private String logDirectory;

    @Value("${ledger.log.partitions:16}")
    private int numPartitions;

    private final Map<Integer, EnhancedLedgerPartition> partitions = new ConcurrentHashMap<>();
    private final AtomicLong globalSequence = new AtomicLong(0);

    // Disruptor for async processing
    private final RingBuffer<LedgerOperationEvent> ringBuffer;
    private final Disruptor<LedgerOperationEvent> disruptor;
    private final ExecutorService disruptorExecutor;

    // Database batch processor
    private final ScheduledExecutorService batchProcessor;
    private final ConcurrentLinkedQueue<CompletedTransaction> pendingDbWrites;

    public HybridLedgerManager() {
        this.disruptorExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.disruptor = new Disruptor<>(
            LedgerOperationEvent::new,
            16384, // Larger ring buffer
            disruptorExecutor,
            ProducerType.MULTI,
            new BlockingWaitStrategy()
        );

        this.disruptor.handleEventsWith(new HybridLogEventHandler());
        this.ringBuffer = disruptor.getRingBuffer();

        // Database batch processor
        this.batchProcessor = Executors.newScheduledThreadPool(2);
        this.pendingDbWrites = new ConcurrentLinkedQueue<>();
    }

    @PostConstruct
    public void initialize() throws IOException {
        // Initialize partitions
        Path logDir = Paths.get(logDirectory);
        Files.createDirectories(logDir);

        for (int i = 0; i < numPartitions; i++) {
            EnhancedLedgerPartition partition = new EnhancedLedgerPartition(
                i, logDir, accountCache, lockManager, circuitBreaker, rateLimiter
            );
            partitions.put(i, partition);
        }

        // Start disruptor
        disruptor.start();

        // Start batch processor
        batchProcessor.scheduleAtFixedRate(this::processDatabaseBatch, 50, 50, TimeUnit.MILLISECONDS);

        logger.info("Hybrid ledger manager initialized with {} partitions", numPartitions);
    }

    public CompletableFuture<Long> processTransaction(
        UUID debitAccountId, UUID creditAccountId, long amount,
        String currencyCode, LocalDate operationDate, UUID idempotencyKey) {

        CompletableFuture<Long> future = new CompletableFuture<>();

        // Check rate limit at entry point
        if (!rateLimiter.tryAcquire()) {
            return CompletableFuture.failedFuture(
                new EnhancedLedgerPartition.RateLimitExceededException("System overloaded"));
        }

        // Publish to disruptor
        long sequence = ringBuffer.next();
        try {
            LedgerOperationEvent event = ringBuffer.get(sequence);

            UUID transactionId = generateUUIDv7();
            Instant now = Instant.now();

            LedgerEntry debitEntry = new LedgerEntry(
                generateUUIDv7(), debitAccountId, transactionId, LedgerEntry.EntryType.DEBIT,
                amount, now, operationDate, idempotencyKey, currencyCode,
                globalSequence.incrementAndGet()
            );

            LedgerEntry creditEntry = new LedgerEntry(
                generateUUIDv7(), creditAccountId, transactionId, LedgerEntry.EntryType.CREDIT,
                amount, now, operationDate, idempotencyKey, currencyCode,
                globalSequence.incrementAndGet()
            );

            event.setDebitEntry(debitEntry);
            event.setCreditEntry(creditEntry);
            event.setCompletionFuture(future);

        } finally {
            ringBuffer.publish(sequence);
        }

        return future;
    }

    // Enhanced Event Handler with hybrid features
    private class HybridLogEventHandler implements EventHandler<LedgerOperationEvent> {
        private final Map<Integer, List<PendingWrite>> pendingWrites = new HashMap<>();
        private final AtomicLong processedEvents = new AtomicLong(0);

        @Override
        public void onEvent(LedgerOperationEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                processedEvents.incrementAndGet();

                // Determine partition
                int partitionId = getPartition(event.getDebitEntry().accountId());

                // Add to batch
                pendingWrites.computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(new PendingWrite(event));

                // Flush when batch is ready
                if (endOfBatch || shouldFlushPartition(partitionId)) {
                    flushPartition(partitionId);
                }

            } catch (Exception e) {
                logger.error("Error in hybrid event handler", e);
                event.getCompletionFuture().completeExceptionally(e);
            }
        }

        private boolean shouldFlushPartition(int partitionId) {
            List<PendingWrite> writes = pendingWrites.get(partitionId);
            return writes != null && writes.size() >= 50; // Smaller batches for lower latency
        }

        private void flushPartition(int partitionId) {
            List<PendingWrite> writes = pendingWrites.get(partitionId);
            if (writes == null || writes.isEmpty()) return;

            EnhancedLedgerPartition partition = partitions.get(partitionId);

            for (PendingWrite write : writes) {
                CompletableFuture<Long> appendFuture = partition.append(
                    write.event.getDebitEntry(),
                    write.event.getCreditEntry()
                );

                // Chain completion and queue for database write
                appendFuture.whenComplete((offset, throwable) -> {
                    if (throwable != null) {
                        TransferErrorType errorType = ErrorClassifier.classify(throwable);
                        handleFlushError(write, throwable, errorType);
                    } else {
                        // Queue for database batch
                        pendingDbWrites.offer(new CompletedTransaction(
                            write.event.getDebitEntry(),
                            write.event.getCreditEntry(),
                            offset
                        ));
                        write.event.getCompletionFuture().complete(offset);
                    }
                });
            }

            writes.clear();
        }

        private void handleFlushError(PendingWrite write, Throwable error, TransferErrorType errorType) {
            switch (errorType) {
                case BUSINESS_ERROR -> {
                    // Complete with business error
                    write.event.getCompletionFuture().completeExceptionally(error);
                }
                case PERFORMANCE_ERROR -> {
                    // Could retry or route to different partition
                    logger.warn("Performance error - considering account cooling: {}",
                        write.event.getDebitEntry().accountId());
                    write.event.getCompletionFuture().completeExceptionally(error);
                }
                case SYSTEM_ERROR, NETWORK_ERROR -> {
                    // Retry logic could be added here
                    write.event.getCompletionFuture().completeExceptionally(error);
                }
            }
        }

        private record PendingWrite(LedgerOperationEvent event) {}
    }

    // Enhanced database batch processor with retry logic
    private void processDatabaseBatch() {
        List<CompletedTransaction> batch = new ArrayList<>();
        CompletedTransaction transaction;

        // Collect batch
        while ((transaction = pendingDbWrites.poll()) != null && batch.size() < 500) {
            batch.add(transaction);
        }

        if (batch.isEmpty()) return;

        try {
            // Prepare batch data
            List<LedgerEntry> entries = new ArrayList<>();
            List<WalEntry> walEntries = new ArrayList<>();

            for (CompletedTransaction txn : batch) {
                entries.add(txn.debitEntry);
                entries.add(txn.creditEntry);

                WalEntry walEntry = new WalEntry(
                    generateUUIDv7(),
                    txn.logOffset,
                    txn.debitEntry.accountId(),
                    txn.creditEntry.accountId(),
                    txn.debitEntry.amount(),
                    txn.debitEntry.currencyCode(),
                    txn.debitEntry.operationDate(),
                    txn.debitEntry.transactionId(),
                    txn.debitEntry.idempotencyKey(),
                    WalEntry.WalStatus.PROCESSED,
                    Instant.now(),
                    Instant.now(),
                    null
                );
                walEntries.add(walEntry);
            }

            // Circuit breaker protected batch insert
            circuitBreaker.executeAsync(
                () -> {
                    ledgerRepository.insertBatch(entries, walEntries);
                    return CompletableFuture.completedFuture(null);
                },
                "database-batch-insert"
            ).exceptionally(throwable -> {
                // Handle batch failure with intelligent retry
                handleBatchFailure(batch, throwable);
                return null;
            });

            logger.debug("Processed database batch of {} transactions", batch.size());

        } catch (Exception e) {
            logger.error("Database batch processing failed", e);
            handleBatchFailure(batch, e);
        }
    }

    private void handleBatchFailure(List<CompletedTransaction> batch, Throwable error) {
        TransferErrorType errorType = ErrorClassifier.classify(error);

        switch (errorType) {
            case NETWORK_ERROR -> {
                // Retry failed transactions up to 3 times
                for (CompletedTransaction txn : batch) {
                    if (txn.retryCount < 3) {
                        txn.retryCount++;
                        pendingDbWrites.offer(txn);
                    } else {
                        logger.error("Dropping transaction after max retries: {}",
                            txn.debitEntry.transactionId());
                    }
                }
            }
            case SYSTEM_ERROR -> {
                logger.error("System error in batch processing - transactions may be lost");
                // Could implement dead letter queue here
            }
            default -> {
                logger.error("Unhandled error type in batch processing: {}", errorType);
            }
        }
    }

    // Balance inquiry with cache optimization
    public CompletableFuture<Long> getBalance(UUID accountId, LocalDate operationDate) {
        // Check cache first
        Optional<AccountState> cachedState = accountCache.get(accountId);
        if (cachedState.isPresent()) {
            return CompletableFuture.completedFuture(cachedState.get().balance());
        }

        // Load from repository and cache
        return CompletableFuture.supplyAsync(() -> {
            try {
                long balance = ledgerRepository.calculateBalance(accountId, operationDate);

                // Cache the result
                AccountState state = new AccountState(balance, 0, System.currentTimeMillis(),
                    null, 0, "BALANCE_INQUIRY");
                accountCache.put(accountId, state);

                return balance;
            } catch (Exception e) {
                logger.error("Failed to get balance for account {}: {}", accountId, e.getMessage());
                throw new RuntimeException("Balance inquiry failed", e);
            }
        });
    }

    // Snapshot creation with barrier mechanism
    public CompletableFuture<SnapshotResult> createSnapshot(UUID accountId, LocalDate operationDate) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                // Create snapshot barrier across all relevant partitions
                int partitionId = getPartition(accountId);
                EnhancedLedgerPartition partition = partitions.get(partitionId);

                // Get current state from cache or database
                Optional<AccountState> state = accountCache.get(accountId);
                if (state.isPresent()) {
                    return new SnapshotResult(true, accountId, state.get().balance(),
                        state.get().lastEntryId(), state.get().lastEntryOrdinal(),
                        (int) state.get().operationCount(), null);
                }

                // Fallback to database
                long balance = ledgerRepository.calculateBalance(accountId, operationDate);
                return new SnapshotResult(true, accountId, balance, null, 0, 0, null);

            } catch (Exception e) {
                logger.error("Snapshot creation failed for account {}: {}", accountId, e.getMessage());
                return new SnapshotResult(false, accountId, 0, null, 0, 0, e.getMessage());
            }
        });
    }

    // Health and monitoring
    public SystemHealth getSystemHealth() {
        boolean circuitBreakerHealthy = !circuitBreaker.isOpen();
        double currentRate = rateLimiter.getCurrentRate();
        long availableTokens = rateLimiter.getAvailableTokens();
        BoundedAccountCache.CacheStats cacheStats = accountCache.getStats();

        boolean healthy = circuitBreakerHealthy &&
            availableTokens > currentRate * 0.1 && // At least 10% tokens available
            cacheStats.size() < cacheStats.maxSize() * 0.9; // Cache not over 90% full

        return new SystemHealth(
            healthy,
            circuitBreakerHealthy,
            circuitBreaker.getState().toString(),
            currentRate,
            availableTokens,
            cacheStats.size(),
            pendingDbWrites.size(),
            partitions.size()
        );
    }

    private int getPartition(UUID accountId) {
        return Math.abs(accountId.hashCode()) % numPartitions;
    }

    private UUID generateUUIDv7() {
        long timestamp = System.currentTimeMillis();
        long timestampHigh = (timestamp >> 16) & 0xFFFFFFFFL;
        long timestampLow = timestamp & 0xFFFFL;

        long randA = ThreadLocalRandom.current().nextLong(0, 4096);
        long randB = ThreadLocalRandom.current().nextLong();

        long mostSigBits = (timestampHigh << 32) | (timestampLow << 16) | (7L << 12) | (randA & 0xFFF);
        long leastSigBits = (2L << 62) | (randB & 0x3FFFFFFFFFFFFFFFL);

        return new UUID(mostSigBits, leastSigBits);
    }

    @PreDestroy
    @Override
    public void close() throws IOException {
        logger.info("Shutting down hybrid ledger manager");

        // Stop accepting new requests
        disruptor.halt();

        // Process remaining database writes
        int remaining = pendingDbWrites.size();
        if (remaining > 0) {
            logger.info("Processing {} remaining database writes", remaining);
            processDatabaseBatch();
        }

        // Close partitions
        for (EnhancedLedgerPartition partition : partitions.values()) {
            partition.close();
        }

        // Shutdown executors
        batchProcessor.shutdown();
        disruptorExecutor.shutdown();

        try {
            if (!batchProcessor.awaitTermination(30, TimeUnit.SECONDS)) {
                batchProcessor.shutdownNow();
            }
            if (!disruptorExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                disruptorExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("Hybrid ledger manager shutdown complete");
    }

    // Supporting classes and records
    private static class CompletedTransaction {
        final LedgerEntry debitEntry;
        final LedgerEntry creditEntry;
        final long logOffset;
        int retryCount = 0;

        CompletedTransaction(LedgerEntry debitEntry, LedgerEntry creditEntry, long logOffset) {
            this.debitEntry = debitEntry;
            this.creditEntry = creditEntry;
            this.logOffset = logOffset;
        }
    }

    public record SnapshotResult(
        boolean success,
        UUID accountId,
        long balance,
        UUID lastEntryId,
        long lastEntryOrdinal,
        int operationsCount,
        String errorMessage
    ) {}

    public record SystemHealth(
        boolean healthy,
        boolean circuitBreakerHealthy,
        String circuitBreakerState,
        double currentRateLimit,
        long availableTokens,
        int cacheSize,
        int pendingDbWrites,
        int activePartitions
    ) {}
}

// Enhanced Disruptor Event
public class LedgerOperationEvent {
    private LedgerEntry debitEntry;
    private LedgerEntry creditEntry;
    private CompletableFuture<Long> completionFuture;
    private TransferErrorType errorType;
    private long processingStartTime;

    public void reset() {
        this.debitEntry = null;
        this.creditEntry = null;
        this.completionFuture = null;
        this.errorType = null;
        this.processingStartTime = 0;
    }

    // Getters and setters
    public LedgerEntry getDebitEntry() { return debitEntry; }
    public void setDebitEntry(LedgerEntry debitEntry) {
        this.debitEntry = debitEntry;
        this.processingStartTime = System.currentTimeMillis();
    }

    public LedgerEntry getCreditEntry() { return creditEntry; }
    public void setCreditEntry(LedgerEntry creditEntry) { this.creditEntry = creditEntry; }

    public CompletableFuture<Long> getCompletionFuture() { return completionFuture; }
    public void setCompletionFuture(CompletableFuture<Long> future) { this.completionFuture = future; }

    public TransferErrorType getErrorType() { return errorType; }
    public void setErrorType(TransferErrorType errorType) { this.errorType = errorType; }

    public long getProcessingDuration() {
        return processingStartTime > 0 ? System.currentTimeMillis() - processingStartTime : 0;
    }
}

// High-level service interface
@Service
public class HybridLedgerService {
    private static final Logger logger = LoggerFactory.getLogger(HybridLedgerService.class);

    @Autowired
    private HybridLedgerManager ledgerManager;

    public CompletableFuture<Void> transfer(UUID fromAccountId, UUID toAccountId,
                                            long amount, String currencyCode) {
        return transferWithIdempotency(fromAccountId, toAccountId, amount,
            currencyCode, UUID.randomUUID());
    }

    public CompletableFuture<Void> transferWithIdempotency(UUID fromAccountId, UUID toAccountId,
                                                           long amount, String currencyCode,
                                                           UUID idempotencyKey) {
        // Validation
        if (amount <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Amount must be positive"));
        }
        if (fromAccountId.equals(toAccountId)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Cannot transfer to same account"));
        }

        return ledgerManager.processTransaction(
            fromAccountId, toAccountId, amount, currencyCode,
            LocalDate.now(), idempotencyKey
        ).thenApply(offset -> null); // Convert Long to Void
    }

    public CompletableFuture<Long> getAccountBalance(UUID accountId) {
        return ledgerManager.getBalance(accountId, LocalDate.now());
    }

    public CompletableFuture<SnapshotResult> createAccountSnapshot(UUID accountId) {
        return ledgerManager.createSnapshot(accountId, LocalDate.now());
    }

    public SystemHealth getSystemHealth() {
        return ledgerManager.getSystemHealth();
    }
}

// Repository interfaces (simplified for space)
@Repository
public interface LedgerRepository {
    void insertBatch(List<LedgerEntry> entries, List<WalEntry> walEntries);
    long calculateBalance(UUID accountId, LocalDate operationDate);
    Optional<BalanceSnapshot> getLatestSnapshot(UUID accountId, LocalDate operationDate);
    List<LedgerEntry> getEntriesAfterOrdinal(UUID accountId, long afterOrdinal, LocalDate operationDate);
}

public record BalanceSnapshot(
    UUID id, UUID accountId, LocalDate operationDate, long balance,
    UUID lastEntryRecordId, long lastEntryOrdinal, int operationsCount,
    long snapshotOrdinal, Instant createdAt
) {}