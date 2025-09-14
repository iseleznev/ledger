package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Production-ready configuration properties
 */
@Data
@ConfigurationProperties(prefix = "ledger.durability")
@Component
public class DurabilityConfiguration {

    @Value("${ledger.durability.base-directory:./ledger-data}")
    private String baseDirectory = "./ledger-data";

    @Value("${ledger.durability.journal.segment-size:134217728}") // 128MB
    private long journalSegmentSize = 128L * 1024 * 1024;

    @Value("${ledger.durability.journal.max-segments:100}")
    private int maxJournalSegments = 100;

    @Value("${ledger.durability.snapshot.interval-operations:10000}")
    private int snapshotIntervalOperations = 10_000;

    @Value("${ledger.durability.snapshot.max-snapshots:10}")
    private int maxSnapshots = 10;

    @Value("${ledger.durability.flush-interval-ms:1000}")
    private long flushIntervalMs = 1000L;

    @Value("${ledger.durability.recovery.parallel-workers:4}")
    private int recoveryParallelWorkers = 4;

    @Value("${ledger.durability.balance-cache.max-size:1000000}")
    private int balanceCacheMaxSize = 1_000_000;
}
