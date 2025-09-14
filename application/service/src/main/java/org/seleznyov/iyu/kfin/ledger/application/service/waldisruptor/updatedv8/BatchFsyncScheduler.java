package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Batch fsync scheduler для групповых синхронизаций
 */
@Slf4j
public class BatchFsyncScheduler {

    private static final BatchFsyncScheduler INSTANCE = new BatchFsyncScheduler();

    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1, Thread.ofVirtual().name("batch-fsync-").factory());

    private final ConcurrentLinkedQueue<Runnable> pendingFsyncs = new ConcurrentLinkedQueue<>();
    private volatile boolean scheduled = false;

    private BatchFsyncScheduler() {
        // Private constructor
    }

    public static BatchFsyncScheduler getInstance() {
        return INSTANCE;
    }

    public void schedule(Runnable fsyncTask) {
        pendingFsyncs.offer(fsyncTask);

        if (!scheduled) {
            synchronized (this) {
                if (!scheduled) {
                    scheduled = true;
                    scheduler.schedule(this::executeBatch, 1, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    private void executeBatch() {
        Runnable task;
        while ((task = pendingFsyncs.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                log.error("Batch fsync task failed", e);
            }
        }
        scheduled = false;
    }
}
