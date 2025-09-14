package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Example usage demonstrating Virtual Threads performance with ledger operations
 */
@Slf4j
public final class VirtualThreadLedgerService {

    /**
     * TODO: Convert to proper integration test with testcontainers
     * TODO: Add performance benchmarking with JMH
     * TODO: Add memory usage analysis
     */
    public static void demonstrateUsage(DataSource dataSource, TransactionTemplate transactionTemplate) {
        log.info("Starting Virtual Thread Ledger Service Demo");
        log.info("Current thread is virtual: {}", Thread.currentThread().isVirtual());

        // Create WAL service with optimal settings for Virtual Threads
        try (var walService = new VirtualThreadLedgerWalService(
            dataSource,
            transactionTemplate,
            64 * 1024,    // 64K ring buffer
            500,          // Batch size
            50L           // Fast flush for Virtual Threads
        )) {

            // Create WAL processor with Virtual Thread support
            try (var walProcessor = new VirtualThreadWalProcessorService(
                new NamedParameterJdbcTemplate(dataSource),
                transactionTemplate
            )) {

                // Start automatic processing
                walProcessor.startScheduledProcessing(100L, 100);

                // Single operation example
                var debitAccount = UUID.randomUUID();
                var creditAccount = UUID.randomUUID();

                long sequenceNumber = walService.writeDoubleEntry(
                    debitAccount,
                    creditAccount,
                    100000L // 1000.00 RUB in kopecks
                );

                log.info("Written WAL entry with sequence: {} in VT: {}",
                    sequenceNumber, walService.isVirtualThread());

                // High-throughput batch operations using Virtual Threads
                var operations = new ArrayList<DoubleEntryRequest>();
                for (int i = 0; i < 10000; i++) {
                    operations.add(new DoubleEntryRequest(
                        UUID.randomUUID(),
                        UUID.randomUUID(),
                        ThreadLocalRandom.current().nextLong(1000L, 100000L)
                    ));
                }

                // Asynchronous processing with Virtual Threads
                var futureSequences = walService.writeDoubleEntriesAsync(operations);
                log.info("Submitted {} operations for async processing...", operations.size());

                // Wait for completion
                var sequenceNumbers = futureSequences.get(10, TimeUnit.SECONDS);
                log.info("Completed {} async operations", sequenceNumbers.size());

                // Parallel WAL processing demonstration
                var parallelProcessingFuture = walProcessor.processInParallel(8, 100);
                int processedCount = parallelProcessingFuture.get(15, TimeUnit.SECONDS);
                log.info("Parallel processing completed: {} entries processed", processedCount);

                // Give time to complete processing
                Thread.sleep(2000);

                // Display performance statistics
                displayStatistics(walService);

                // High load stress test
                performStressTest(walService, walProcessor);
            }
        }

        log.info("Virtual Thread Ledger Service Demo completed");
    }

    /**
     * TODO: Add more detailed performance metrics
     * TODO: Integrate with Micrometer for metrics collection
     */
    private static void displayStatistics(VirtualThreadLedgerWalService walService) {
        log.info("\n=== Virtual Thread WAL Service Statistics ===");
        log.info("Total WAL processed: {}", walService.getProcessedCount());
        log.info("Ring buffer size: {}", walService.getRingBufferSize());
        log.info("Ring buffer remaining: {}", walService.getRingBufferRemaining());
        log.info("Running in Virtual Thread: {}", Thread.currentThread().isVirtual());
        log.info("Available processors: {}", Runtime.getRuntime().availableProcessors());
    }

    /**
     * TODO: Make stress test parameters configurable
     * TODO: Add different load patterns (burst, sustained, ramp-up)
     * TODO: Add latency percentile measurements
     */
    private static void performStressTest(
        VirtualThreadLedgerWalService walService,
        VirtualThreadWalProcessorService walProcessor) {

        log.info("\n=== High Load Stress Test with Virtual Threads ===");

        long startTime = System.nanoTime();
        final int VIRTUAL_THREADS = 1000;
        final int OPERATIONS_PER_THREAD = 100;
        final int TOTAL_OPERATIONS = VIRTUAL_THREADS * OPERATIONS_PER_THREAD;

        try (var virtualExecutor = Executors.newVirtualThreadPerTaskExecutor()) {

            // Create Virtual Threads for massive concurrent load
            var futures = new ArrayList<CompletableFuture<Void>>(VIRTUAL_THREADS);

            for (int threadNum = 0; threadNum < VIRTUAL_THREADS; threadNum++) {
                final int threadId = threadNum;

                var future = CompletableFuture.runAsync(() -> {
                    // Each Virtual Thread performs multiple operations
                    for (int op = 0; op < OPERATIONS_PER_THREAD; op++) {
                        walService.writeDoubleEntry(
                            UUID.randomUUID(),
                            UUID.randomUUID(),
                            ThreadLocalRandom.current().nextLong(1000L, 50000L)
                        );
                    }

                    // Log progress every 100 threads
                    if (threadId % 100 == 0) {
                        log.debug("VT-{}: Completed {} operations", threadId, OPERATIONS_PER_THREAD);
                    }
                }, virtualExecutor);

                futures.add(future);
            }

            // Wait for all Virtual Threads to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .get(60, TimeUnit.SECONDS);

            long endTime = System.nanoTime();
            double durationMs = (endTime - startTime) / 1_000_000.0;
            double throughputOpsPerSec = TOTAL_OPERATIONS / (durationMs / 1000.0);

            log.info("Stress Test Results:");
            log.info("  Virtual Threads: {}", VIRTUAL_THREADS);
            log.info("  Operations per thread: {}", OPERATIONS_PER_THREAD);
            log.info("  Total operations: {}", TOTAL_OPERATIONS);
            log.info("  Duration: {:.2f} ms", durationMs);
            log.info("  Throughput: {:.0f} ops/sec", throughputOpsPerSec);
            log.info("  Average latency: {:.3f} ms/op", durationMs / TOTAL_OPERATIONS);

            // Allow time for WAL processing
            Thread.sleep(3000);

            // Process remaining WAL entries
            log.info("Processing remaining WAL entries...");
            int finalProcessed = walProcessor.processInParallel(16, 200).get(30, TimeUnit.SECONDS);
            log.info("Final processing completed: {} entries", finalProcessed);

        } catch (Exception e) {
            log.error("ERROR in stress test", e);
        }
    }

    /**
     * Memory and performance monitoring utilities for Virtual Threads
     * <p>
     * TODO: Integrate with JVM Flight Recorder for detailed profiling
     * TODO: Add GC analysis specific to Virtual Thread usage
     * TODO: Monitor carrier thread utilization
     */
    public static void monitorVirtualThreadPerformance() {
        var runtime = Runtime.getRuntime();
        var memoryMXBean = java.lang.management.ManagementFactory.getMemoryMXBean();

        log.info("\n=== Virtual Thread Performance Monitoring ===");
        log.info("Available processors: {}", runtime.availableProcessors());
        log.info("Max heap memory: {} MB", runtime.maxMemory() / (1024 * 1024));
        log.info("Total heap memory: {} MB", runtime.totalMemory() / (1024 * 1024));
        log.info("Free heap memory: {} MB", runtime.freeMemory() / (1024 * 1024));

        var heapUsage = memoryMXBean.getHeapMemoryUsage();
        log.info("Heap usage: {} MB / {} MB ({:.1f}%)",
            heapUsage.getUsed() / (1024 * 1024),
            heapUsage.getMax() / (1024 * 1024),
            (double) heapUsage.getUsed() / heapUsage.getMax() * 100);

        // Thread information
        var threadMXBean = java.lang.management.ManagementFactory.getThreadMXBean();
        log.info("Active threads: {}", threadMXBean.getThreadCount());
        log.info("Peak threads: {}", threadMXBean.getPeakThreadCount());
        log.info("Total started threads: {}", threadMXBean.getTotalStartedThreadCount());

        // TODO: Add Virtual Thread specific monitoring
        // - Carrier thread pool utilization
        // - Virtual Thread creation rate
        // - Pinning events monitoring
        // - Virtual Thread memory overhead
    }
}
