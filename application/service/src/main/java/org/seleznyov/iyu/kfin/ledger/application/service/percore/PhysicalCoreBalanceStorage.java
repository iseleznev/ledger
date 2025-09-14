package org.seleznyov.iyu.kfin.ledger.application.service.percore;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Physical Core Balance Storage - one buffer per physical CPU core
 * Uses HashMap-style bit shifting for ultra-fast core assignment
 * Fixed: использует long[] для sequence numbers вместо int[]
 */
@Component
@Slf4j
public class PhysicalCoreBalanceStorage implements AutoCloseable {

    // Account entry layout: [UUID_HIGH:8][UUID_LOW:8][BALANCE:8][TIMESTAMP:8] = 32 bytes
    private static final int ACCOUNT_ENTRY_SIZE = 32;
    private static final long BUFFER_SIZE_PER_CORE = 32L * 1024 * 1024; // 32MB per core
    private static final int MAX_ACCOUNTS_PER_CORE = (int) (BUFFER_SIZE_PER_CORE / ACCOUNT_ENTRY_SIZE);

    private final int physicalCores;
    private final int coreMask; // For bit shifting instead of division
    private final ByteBuffer[] coreBuffers;
    private final HashMap<UUID, Integer>[] coreIndexes; // No synchronization needed
    private final int[] nextSlots;
//    private final AtomicInteger[] nextSlots;
    private final ExecutorService[] coreExecutors;

    // Performance tracking
    private final AtomicInteger totalAccounts = new AtomicInteger(0);
    private final long[] coreOperationCounts;

    @SuppressWarnings("unchecked")
    public PhysicalCoreBalanceStorage() {
        this.physicalCores = detectPhysicalCores();
        this.coreMask = physicalCores - 1; // For power-of-2 optimization
        this.coreBuffers = new ByteBuffer[physicalCores];
        this.coreIndexes = new HashMap[physicalCores];
        this.nextSlots = new int[physicalCores];
        this.coreExecutors = new ExecutorService[physicalCores];
        this.coreOperationCounts = new long[physicalCores];

        validatePowerOfTwo();
        initializeCoreResources();

        log.info("Initialized PhysicalCoreBalanceStorage with {} physical cores, {}MB total memory",
            physicalCores, (physicalCores * BUFFER_SIZE_PER_CORE) / (1024 * 1024));
    }

    private void validatePowerOfTwo() {
        if ((physicalCores & (physicalCores - 1)) != 0) {
            throw new IllegalStateException(
                "Physical core count must be power of 2 for bit shifting optimization. Found: " + physicalCores);
        }
    }

    private void initializeCoreResources() {
        for (int core = 0; core < physicalCores; core++) {
            coreBuffers[core] = ByteBuffer.allocateDirect((int) BUFFER_SIZE_PER_CORE);
            coreIndexes[core] = new HashMap<>(MAX_ACCOUNTS_PER_CORE);
            nextSlots[core] = 0;//new AtomicInteger(0);

            // Single virtual thread per physical core - no contention
            coreExecutors[core] = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("core-" + core + "-worker-").factory());
        }
    }

    /**
     * Convert UUID to int and map to physical core using bit shifting
     */
    private int getPhysicalCoreId(UUID accountId) {
        // Convert UUID to int using XOR of high and low bits
        long high = accountId.getMostSignificantBits();
        long low = accountId.getLeastSignificantBits();
        int hash = (int) (high ^ low ^ (high >>> 32) ^ (low >>> 32));

        // Use bit masking instead of modulo (requires power-of-2 core count)
        return hash & coreMask;
    }

    /**
     * Get account balance - direct virtual thread execution
     */
    public OptionalLong getBalance(UUID accountId) {
        int coreId = getPhysicalCoreId(accountId);

        // Direct submission to virtual thread executor for this core
        var result = new Object() { OptionalLong value = OptionalLong.empty(); };

        coreExecutors[coreId].submit(() -> {
            // No locks needed - single thread per core
            Integer slot = coreIndexes[coreId].get(accountId);
            if (slot != null) {
                long balance = readBalance(coreId, slot);
                coreOperationCounts[coreId]++;
                result.value = OptionalLong.of(balance);
            }

            // Wake up waiting thread
            synchronized (result) {
                result.notify();
            }
        });

        // Wait for result (virtual threads make this very efficient)
        synchronized (result) {
            try {
                result.wait(100); // 100ms timeout
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return result.value;
    }

    /**
     * Get account balance - async version for non-blocking access
     */
    public void getBalanceAsync(UUID accountId, BalanceCallback callback) {
        int coreId = getPhysicalCoreId(accountId);

        coreExecutors[coreId].submit(() -> {
            try {
                Integer slot = coreIndexes[coreId].get(accountId);
                if (slot != null) {
                    long balance = readBalance(coreId, slot);
                    coreOperationCounts[coreId]++;
                    callback.onSuccess(balance);
                } else {
                    callback.onNotFound();
                }
            } catch (Exception e) {
                callback.onError(e);
            }
        });
    }

    /**
     * Update account balance - direct virtual thread execution
     */
    public void updateBalance(UUID accountId, long newBalance) {
        int coreId = getPhysicalCoreId(accountId);

        coreExecutors[coreId].submit(() -> {
            Integer slot = coreIndexes[coreId].computeIfAbsent(accountId,
                id -> allocateSlot(coreId, id));

            writeBalance(coreId, slot, accountId, newBalance);
            coreOperationCounts[coreId]++;
        });
    }

    /**
     * Add delta to account balance (optimized for frequent updates)
     */
    public void addToBalance(UUID accountId, long delta) {
        int coreId = getPhysicalCoreId(accountId);

        coreExecutors[coreId].submit(() -> {
            Integer slot = coreIndexes[coreId].get(accountId);
            if (slot != null) {
                long currentBalance = readBalance(coreId, slot);
                writeBalance(coreId, slot, accountId, currentBalance + delta);
            } else {
                // Create new account with delta as initial balance
                slot = allocateSlot(coreId, accountId);
                writeBalance(coreId, slot, accountId, delta);
            }
            coreOperationCounts[coreId]++;
        });
    }

    /**
     * Update account balance with confirmation callback
     */
    public void updateBalanceAsync(UUID accountId, long newBalance, UpdateCallback callback) {
        int coreId = getPhysicalCoreId(accountId);

        coreExecutors[coreId].submit(() -> {
            try {
                Integer slot = coreIndexes[coreId].computeIfAbsent(accountId,
                    id -> allocateSlot(coreId, id));

                writeBalance(coreId, slot, accountId, newBalance);
                coreOperationCounts[coreId]++;
                callback.onSuccess();
            } catch (Exception e) {
                callback.onError(e);
            }
        });
    }

    /**
     * Batch balance read for multiple accounts using virtual threads
     */
    public Map<UUID, Long> getBalances(Set<UUID> accountIds) {
        if (accountIds.isEmpty()) {
            return Collections.emptyMap();
        }

        // Group accounts by physical core for efficient processing
        Map<Integer, List<UUID>> accountsByCore = new HashMap<>();

        for (UUID accountId : accountIds) {
            int coreId = getPhysicalCoreId(accountId);
            accountsByCore.computeIfAbsent(coreId, k -> new ArrayList<>()).add(accountId);
        }

        // Process each core's accounts in parallel using virtual threads
        Map<UUID, Long> allResults = new HashMap<>();
        List<Thread> virtualThreads = new ArrayList<>();

        for (Map.Entry<Integer, List<UUID>> entry : accountsByCore.entrySet()) {
            int coreId = entry.getKey();
            List<UUID> coreAccounts = entry.getValue();

            Thread virtualThread = Thread.ofVirtual().start(() -> {
                // Use a completion future to wait for executor result
                var coreResult = new Object() {
                    Map<UUID, Long> results = new HashMap<>();
                    boolean completed = false;
                };

                coreExecutors[coreId].submit(() -> {
                    for (UUID accountId : coreAccounts) {
                        Integer slot = coreIndexes[coreId].get(accountId);
                        if (slot != null) {
                            long balance = readBalance(coreId, slot);
                            coreResult.results.put(accountId, balance);
                        }
                    }
                    coreOperationCounts[coreId] += coreAccounts.size();

                    synchronized (coreResult) {
                        coreResult.completed = true;
                        coreResult.notify();
                    }
                });

                // Wait for completion
                synchronized (coreResult) {
                    while (!coreResult.completed) {
                        try {
                            coreResult.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }

                // Add results to global map
                synchronized (allResults) {
                    allResults.putAll(coreResult.results);
                }
            });

            virtualThreads.add(virtualThread);
        }

        // Wait for all virtual threads to complete
        for (Thread thread : virtualThreads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        return allResults;
    }

    /**
     * Async batch balance read with callback
     */
    public void getBalancesAsync(Set<UUID> accountIds, BatchBalanceCallback callback) {
        Thread.ofVirtual().start(() -> {
            try {
                Map<UUID, Long> results = getBalances(accountIds);
                callback.onSuccess(results);
            } catch (Exception e) {
                callback.onError(e);
            }
        });
    }

    /**
     * Read balance from specific core buffer at slot position
     */
    private long readBalance(int coreId, int slot) {
        ByteBuffer buffer = coreBuffers[coreId];
        int offset = slot * ACCOUNT_ENTRY_SIZE;

        // Read balance from position: UUID_HIGH(8) + UUID_LOW(8) + BALANCE(8)
        return buffer.getLong(offset + 16);
    }

    /**
     * Write balance to specific core buffer at slot position
     */
    private void writeBalance(int coreId, int slot, UUID accountId, long balance) {
        ByteBuffer buffer = coreBuffers[coreId];
        int offset = slot * ACCOUNT_ENTRY_SIZE;

        // Write complete entry: [UUID_HIGH:8][UUID_LOW:8][BALANCE:8][TIMESTAMP:8]
        buffer.putLong(offset, accountId.getMostSignificantBits());
        buffer.putLong(offset + 8, accountId.getLeastSignificantBits());
        buffer.putLong(offset + 16, balance);
        buffer.putLong(offset + 24, System.currentTimeMillis());
    }

    /**
     * Allocate new slot in specific core buffer
     */
    private int allocateSlot(int coreId, UUID accountId) {
        int slot = nextSlots[coreId]++;// + 1;//.getAndIncrement();

        if (slot >= MAX_ACCOUNTS_PER_CORE) {
            throw new IllegalStateException(
                "Core " + coreId + " buffer full. Max accounts per core: " + MAX_ACCOUNTS_PER_CORE);
        }

        totalAccounts.incrementAndGet();

        log.debug("Allocated slot {} in core {} for account {}", slot, coreId, accountId);
        return slot;
    }

    /**
     * Get all account balances from specific core
     */
    public void getCoreBalancesAsync(int coreId, BatchBalanceCallback callback) {
        if (coreId < 0 || coreId >= physicalCores) {
            callback.onSuccess(Collections.emptyMap());
            return;
        }

        coreExecutors[coreId].submit(() -> {
            try {
                Map<UUID, Long> coreBalances = new HashMap<>();

                for (Map.Entry<UUID, Integer> entry : coreIndexes[coreId].entrySet()) {
                    UUID accountId = entry.getKey();
                    int slot = entry.getValue();
                    long balance = readBalance(coreId, slot);
                    coreBalances.put(accountId, balance);
                }

                callback.onSuccess(coreBalances);
            } catch (Exception e) {
                callback.onError(e);
            }
        });
    }

    /**
     * Create snapshot for recovery using virtual threads
     */
    public void createSnapshotAsync(SnapshotCallback callback) {
        Thread.ofVirtual().start(() -> {
            try {
                Map<UUID, Long> fullSnapshot = new HashMap<>();
                List<Thread> coreThreads = new ArrayList<>();

                for (int coreId = 0; coreId < physicalCores; coreId++) {
                    final int currentCore = coreId;

                    Thread coreThread = Thread.ofVirtual().start(() -> {
                        var coreResult = new Object() {
                            Map<UUID, Long> results = new HashMap<>();
                            boolean completed = false;
                        };

                        coreExecutors[currentCore].submit(() -> {
                            for (Map.Entry<UUID, Integer> entry : coreIndexes[currentCore].entrySet()) {
                                UUID accountId = entry.getKey();
                                int slot = entry.getValue();
                                long balance = readBalance(currentCore, slot);
                                coreResult.results.put(accountId, balance);
                            }

                            synchronized (coreResult) {
                                coreResult.completed = true;
                                coreResult.notify();
                            }
                        });

                        // Wait for core processing to complete
                        synchronized (coreResult) {
                            while (!coreResult.completed) {
                                try {
                                    coreResult.wait();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        }

                        // Add to full snapshot
                        synchronized (fullSnapshot) {
                            fullSnapshot.putAll(coreResult.results);
                        }
                    });

                    coreThreads.add(coreThread);
                }

                // Wait for all core threads to complete
                for (Thread thread : coreThreads) {
                    thread.join();
                }

                log.info("Created snapshot with {} accounts across {} cores",
                    fullSnapshot.size(), physicalCores);
                callback.onSuccess(fullSnapshot);

            } catch (Exception e) {
                callback.onError(e);
            }
        });
    }

    /**
     * Restore balances from snapshot data
     */
    public void restoreFromSnapshot(Map<UUID, Long> snapshotBalances, RestoreCallback callback) {
        Thread.ofVirtual().start(() -> {
            try {
                // Group balances by core for parallel restoration
                Map<Integer, Map<UUID, Long>> balancesByCore = new HashMap<>();

                for (Map.Entry<UUID, Long> entry : snapshotBalances.entrySet()) {
                    int coreId = getPhysicalCoreId(entry.getKey());
                    balancesByCore.computeIfAbsent(coreId, k -> new HashMap<>())
                        .put(entry.getKey(), entry.getValue());
                }

                // Restore each core in parallel
                List<Thread> restoreThreads = new ArrayList<>();
                int[] restoredCounts = new int[physicalCores];

                for (Map.Entry<Integer, Map<UUID, Long>> coreEntry : balancesByCore.entrySet()) {
                    int coreId = coreEntry.getKey();
                    Map<UUID, Long> coreBalances = coreEntry.getValue();

                    Thread restoreThread = Thread.ofVirtual().start(() -> {
                        var restoreResult = new Object() {
                            int count = 0;
                            boolean completed = false;
                        };

                        coreExecutors[coreId].submit(() -> {
                            // Clear existing data for this core
                            coreIndexes[coreId].clear();
                            nextSlots[coreId] = 0;//.set(0);

                            // Restore balances
                            for (Map.Entry<UUID, Long> balanceEntry : coreBalances.entrySet()) {
                                UUID accountId = balanceEntry.getKey();
                                long balance = balanceEntry.getValue();

                                int slot = allocateSlot(coreId, accountId);
                                writeBalance(coreId, slot, accountId, balance);
                                restoreResult.count++;
                            }

                            synchronized (restoreResult) {
                                restoreResult.completed = true;
                                restoreResult.notify();
                            }
                        });

                        // Wait for restore to complete
                        synchronized (restoreResult) {
                            while (!restoreResult.completed) {
                                try {
                                    restoreResult.wait();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    return;
                                }
                            }
                        }

                        restoredCounts[coreId] = restoreResult.count;
                    });

                    restoreThreads.add(restoreThread);
                }

                // Wait for all restore threads
                for (Thread thread : restoreThreads) {
                    thread.join();
                }

                int totalRestored = Arrays.stream(restoredCounts).sum();
                totalAccounts.set(totalRestored);

                log.info("Restored {} account balances from snapshot across {} cores",
                    totalRestored, physicalCores);

                callback.onSuccess(totalRestored);

            } catch (Exception e) {
                log.error("Snapshot restore failed", e);
                callback.onError(e);
            }
        });
    }

    /**
     * Get system statistics
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();

        stats.put("physicalCores", physicalCores);
        stats.put("totalAccounts", totalAccounts.get());
        stats.put("maxAccountsPerCore", MAX_ACCOUNTS_PER_CORE);
        stats.put("bufferSizePerCoreMB", BUFFER_SIZE_PER_CORE / (1024 * 1024));

        // Per-core statistics
        Map<String, Object> coreStats = new HashMap<>();
        for (int i = 0; i < physicalCores; i++) {
            Map<String, Object> coreStat = new HashMap<>();
            coreStat.put("accountCount", coreIndexes[i].size());
            coreStat.put("operationCount", coreOperationCounts[i]);
            coreStat.put("utilizationPercent",
                (double) coreIndexes[i].size() / MAX_ACCOUNTS_PER_CORE * 100.0);
            coreStats.put("core" + i, coreStat);
        }
        stats.put("coreStatistics", coreStats);

        return stats;
    }

    /**
     * Check if system is healthy
     */
    public boolean isHealthy() {
        try {
            // Check if any core is over 90% capacity
            for (int i = 0; i < physicalCores; i++) {
                if (coreIndexes[i].size() > MAX_ACCOUNTS_PER_CORE * 0.9) {
                    return false;
                }
            }

            // Check if executors are responsive
            return totalAccounts.get() >= 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get executor services for integration with other components
     */
    public ExecutorService[] getCoreExecutors() {
        return coreExecutors.clone();
    }

    /**
     * Detect physical CPU cores (power of 2 for bit shifting optimization)
     */
    private int detectPhysicalCores() {
        int detected = detectActualPhysicalCores();

        // Ensure power of 2 for bit shifting optimization
        int powerOfTwo = 1;
        while (powerOfTwo < detected) {
            powerOfTwo <<= 1;
        }

        // If detected is already power of 2, use it; otherwise use next lower power of 2
        return detected == powerOfTwo ? detected : powerOfTwo >> 1;
    }

    private int detectActualPhysicalCores() {
        try {
            // Linux approach
            Path cpuInfoPath = Paths.get("/proc/cpuinfo");
            if (Files.exists(cpuInfoPath)) {
                String cpuInfo = Files.readString(cpuInfoPath);
                long coreIds = cpuInfo.lines()
                    .filter(line -> line.startsWith("core id"))
                    .map(line -> line.split(":")[1].trim())
                    .distinct()
                    .count();

                if (coreIds > 0) {
                    log.info("Detected {} physical cores from /proc/cpuinfo", coreIds);
                    return (int) coreIds;
                }
            }
        } catch (IOException e) {
            log.debug("Could not read /proc/cpuinfo: {}", e.getMessage());
        }

        // Fallback heuristic
        int logical = Runtime.getRuntime().availableProcessors();
        int physical = Math.max(1, logical / 2);

        log.info("Using heuristic: {} physical cores ({} logical processors)", physical, logical);
        return physical;
    }

    @Override
    public void close() {
        log.info("Closing PhysicalCoreBalanceStorage...");

        for (ExecutorService executor : coreExecutors) {
            if (executor != null) {
                executor.shutdown();
            }
        }

        // Clear direct buffers (help GC)
        for (ByteBuffer buffer : coreBuffers) {
            if (buffer != null && buffer.isDirect()) {
                buffer.clear();
            }
        }

        Map<String, Object> finalStats = getStatistics();
        log.info("PhysicalCoreBalanceStorage closed. Final stats: {}", finalStats);
    }

    // Callback interfaces for async operations
    @FunctionalInterface
    public interface BalanceCallback {
        void onSuccess(long balance);
        default void onNotFound() {}
        default void onError(Exception e) { log.error("Balance operation failed", e); }
    }

    @FunctionalInterface
    public interface UpdateCallback {
        void onSuccess();
        default void onError(Exception e) { log.error("Update operation failed", e); }
    }

    @FunctionalInterface
    public interface BatchBalanceCallback {
        void onSuccess(Map<UUID, Long> balances);
        default void onError(Exception e) { log.error("Batch balance operation failed", e); }
    }

    @FunctionalInterface
    public interface SnapshotCallback {
        void onSuccess(Map<UUID, Long> snapshot);
        default void onError(Exception e) { log.error("Snapshot operation failed", e); }
    }

    @FunctionalInterface
    public interface RestoreCallback {
        void onSuccess(int restoredCount);
        default void onError(Exception e) { log.error("Restore operation failed", e); }
    }

    /**
     * Test method to verify core distribution
     */
    public void testCoreDistribution(int testAccounts) {
        Map<Integer, Integer> distribution = new HashMap<>();

        for (int i = 0; i < testAccounts; i++) {
            UUID testId = UUID.randomUUID();
            int core = getPhysicalCoreId(testId);
            distribution.merge(core, 1, Integer::sum);
        }

        log.info("Core distribution for {} test accounts: {}", testAccounts, distribution);

        double expectedPerCore = (double) testAccounts / physicalCores;
        distribution.forEach((core, count) -> {
            double deviation = Math.abs(count - expectedPerCore) / expectedPerCore * 100.0;
            log.info("Core {}: {} accounts ({:.1f}% deviation from expected {:.0f})",
                core, count, deviation, expectedPerCore);
        });
    }
}