package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Production-ready configuration properties
 */
@Data
@ConfigurationProperties(prefix = "ledger.wal")
@Component
public class LedgerWalConfiguration {

    @Value("${ledger.wal.ring-buffer-size:65536}")
    private int ringBufferSize = 64 * 1024; // Must be power of 2

    @Value("${ledger.wal.batch-size:500}")
    private int batchSize = 500;

    @Value("${ledger.wal.flush-interval-ms:50}")
    private long flushIntervalMs = 50L;

    @Value("${ledger.wal.circuit-breaker.failure-threshold:5}")
    private int circuitBreakerFailureThreshold = 5;

    @Value("${ledger.wal.circuit-breaker.timeout-ms:30000}")
    private long circuitBreakerTimeoutMs = 30_000L;

    @Value("${ledger.wal.idempotency-cache.max-size:100000}")
    private int idempotencyCacheMaxSize = 100_000;

    @Value("${ledger.wal.idempotency-cache.eviction-percentage:10}")
    private int idempotencyCacheEvictionPercentage = 10;

    @Value("${ledger.wal.retry.max-attempts:3}")
    private int maxRetryAttempts = 3;

    @Value("${ledger.wal.retry.base-delay-ms:100}")
    private long retryBaseDelayMs = 100L;

    @Value("${ledger.wal.shutdown-timeout-seconds:10}")
    private long shutdownTimeoutSeconds = 10L;

    @Value("${ledger.wal.processor.parallel-workers:8}")
    private int processorParallelWorkers = 8;

    @Value("${ledger.wal.processor.scheduled-interval-ms:100}")
    private long processorScheduledIntervalMs = 100L;
}
