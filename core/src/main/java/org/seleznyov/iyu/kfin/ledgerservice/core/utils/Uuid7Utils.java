package org.seleznyov.iyu.kfin.ledgerservice.core.utils;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class Uuid7Utils {

    private static volatile long baseTimestampMs = System.currentTimeMillis();
    private static volatile long baseNanoTime = System.nanoTime();
    private static volatile long lastSyncNanos = System.nanoTime();
    private static final long SYNC_INTERVAL_NANOS = 10_000_000L; // 10ms

    private Uuid7Utils() {

    }

    public static long uuidToLong(UUID uuid) {
        return uuid.getMostSignificantBits() ^ uuid.getLeastSignificantBits();
    }

    public static UUID nextUuid7Syscall() {
        final long nowMs = System.currentTimeMillis() & 0xFFFFFFFFFFFFL; // 48 бит времени
        final long rndA  = ThreadLocalRandom.current().nextLong();         // для rand_a (12 бит)
        final long rndB  = ThreadLocalRandom.current().nextLong();         // для rand_b (62 бита)

        // MSB: [48 бит времени][4 бита версии=0111][12 бит rand_a]
        long msb = (nowMs << 16);           // сдвигаем timestamp в старшие 48 бит
        msb = msb | 0x7000L;               // version = 7 (битовая маска в позиции 12..15)
        msb = msb | (rndA & 0xFFFL);     // 12 младших бит случайности

        // LSB: [2 бита варианта=10][62 бита rand_b]
        long lsb = rndB & 0x3FFFFFFFFFFFFFFFL; // очистить верхние 2 бита
        lsb = lsb | (1L << 63);               // установить самый старший бит (variant=10)
        // (второй старший остаётся 0 после маски)

        return new UUID(msb, lsb);
    }

    /**
     * Генерирует UUID7 с микросекундной точностью.
     *
     * Структура:
     * - 48 бит: timestamp в миллисекундах
     * - 4 бита: версия (7)
     * - 12 бит: микросекундная часть (0-999 мкс)
     * - 2 бита: variant (10)
     * - 62 бита: random
     *
     * @return UUID версии 7
     */
    public static UUID nextUuid7() {
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

        ThreadLocalRandom random = ThreadLocalRandom.current();

        // Используем микросекундную часть для 12-битного поля
        int microseconds = (int)((nanoOffset % 1_000_000L) / 1000L);

        long mostSigBits = (timestamp << 16) | (0x7L << 12) | (microseconds & 0xFFF);
        long leastSigBits = (random.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        return new UUID(mostSigBits, leastSigBits);
    }
}
