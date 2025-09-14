package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for Container Optimized Ledger Service
 */
@Configuration
@ConfigurationProperties(prefix = "ledger.container-optimized")
@Data
public class LedgerConfiguration {

    private BalanceStorageConfig balanceStorage = new BalanceStorageConfig();
    private WalConfig wal = new WalConfig();
    private SnapshotConfig snapshot = new SnapshotConfig();
    private PerformanceConfig performance = new PerformanceConfig();

    @Data
    public static class BalanceStorageConfig {
        private int shardCount = 0; // 0 = auto-detect
        private int initialCapacityPerShard = 100_000;
        private int threadsPerShard = 2;
        private boolean enableCorePinning = false; // Disabled for containers
    }

    @Data
    public static class WalConfig {
        private int shardCount = 0; // 0 = auto-detect
        private int queueSizePerShard = 50_000;
        private int batchSize = 1000;
        private int flushIntervalSeconds = 2;
        private int maxFlushRetries = 3;
        private DurabilityLevel defaultDurabilityLevel = DurabilityLevel.WAL_BUFFERED;
        private boolean enableChecksumValidation = true;
    }

    @Data
    public static class SnapshotConfig {
        private int shardCount = 0; // 0 = auto-detect
        private int operationsThreshold = 100; // Create snapshot after N operations
        private int initialCapacityPerShard = 10_000;
        private boolean enableAutoSnapshots = true;
        private int maxSnapshotsPerAccount = 10;
        private int cleanupIntervalHours = 24;
    }

    @Data
    public static class PerformanceConfig {
        private int schedulerThreads = 4;
        private int statsLogIntervalSeconds = 30;
        private int healthCheckIntervalSeconds = 15;
        private double minSuccessRatePercent = 95.0;
        private boolean enableDetailedMetrics = false;
        private int operationTimeoutSeconds = 30;
    }

    /**
     * Get optimal shard count based on available processors
     */
    public int getOptimalShardCount(int configuredCount) {
        if (configuredCount > 0) {
            return ensurePowerOfTwo(configuredCount);
        }

        int available = Runtime.getRuntime().availableProcessors();
        int optimal = Math.max(2, available);

        return ensurePowerOfTwo(optimal);
    }

    /**
     * Ensure the count is a power of 2 for efficient bit masking
     */
    private int ensurePowerOfTwo(int value) {
        int powerOfTwo = 1;
        while (powerOfTwo < value) {
            powerOfTwo <<= 1;
        }
        return value == powerOfTwo ? value : powerOfTwo >> 1;
    }

    /**
     * Validate configuration on startup
     */
    public void validate() {
        // Validate balance storage config
        if (balanceStorage.initialCapacityPerShard < 1000) {
            throw new IllegalArgumentException("Balance storage initial capacity too small: " +
                balanceStorage.initialCapacityPerShard);
        }

        if (balanceStorage.threadsPerShard < 1 || balanceStorage.threadsPerShard > 10) {
            throw new IllegalArgumentException("Invalid threads per shard: " +
                balanceStorage.threadsPerShard);
        }

        // Validate WAL config
        if (wal.queueSizePerShard < 1000) {
            throw new IllegalArgumentException("WAL queue size too small: " + wal.queueSizePerShard);
        }

        if (wal.batchSize > wal.queueSizePerShard / 2) {
            throw new IllegalArgumentException("WAL batch size too large relative to queue size");
        }

        if (wal.flushIntervalSeconds < 1 || wal.flushIntervalSeconds > 300) {
            throw new IllegalArgumentException("Invalid WAL flush interval: " +
                wal.flushIntervalSeconds);
        }

        // Validate snapshot config
        if (snapshot.operationsThreshold < 1 || snapshot.operationsThreshold > 10000) {
            throw new IllegalArgumentException("Invalid snapshot operations threshold: " +
                snapshot.operationsThreshold);
        }

        // Validate performance config
        if (performance.minSuccessRatePercent < 50.0 || performance.minSuccessRatePercent > 100.0) {
            throw new IllegalArgumentException("Invalid min success rate: " +
                performance.minSuccessRatePercent);
        }

        if (performance.operationTimeoutSeconds < 1 || performance.operationTimeoutSeconds > 300) {
            throw new IllegalArgumentException("Invalid operation timeout: " +
                performance.operationTimeoutSeconds);
        }
    }
}