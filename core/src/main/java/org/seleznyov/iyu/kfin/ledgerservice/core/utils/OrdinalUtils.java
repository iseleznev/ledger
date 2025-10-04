package org.seleznyov.iyu.kfin.ledgerservice.core.utils;

public class OrdinalUtils {

    private static volatile long baseTimestampMs = System.currentTimeMillis();
    private static volatile long baseNanoTime = System.nanoTime();
    private static volatile long lastSyncNanos = System.nanoTime();
    private static final long SYNC_INTERVAL_NANOS = 10_000_000L; // 10ms

    private OrdinalUtils() {

    }

    public static long nextOrdinal(long currentOrdinal) {
        long currentNanos = System.nanoTime();

        // Периодическая синхронизация (раз в 10ms)
        if (currentNanos - lastSyncNanos >= SYNC_INTERVAL_NANOS) {
            baseTimestampMs = System.currentTimeMillis();
            baseNanoTime = currentNanos;
            lastSyncNanos = currentNanos;
        }

        // Вычисляем текущий timestamp на основе nanoTime offset
        long nanoOffset = currentNanos - baseNanoTime;
        long timestamp = baseTimestampMs + (nanoOffset / 1_000_000L);

        if ((timestamp << 32) == (currentOrdinal & 0xffffffff00000000L)) {
            return currentOrdinal + 1;
        }

        return timestamp << 32;
    }
}
