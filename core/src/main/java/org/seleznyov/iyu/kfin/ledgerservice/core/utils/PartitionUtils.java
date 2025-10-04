package org.seleznyov.iyu.kfin.ledgerservice.core.utils;

import java.util.UUID;

/**
 * Роутер для определения инстанса и партиции счетов.
 * <p>
 * Два независимых метода:
 * 1. getInstanceNumber() - определяет номер инстанса приложения
 * 2. getPartitionNumber() - определяет номер партиции
 * <p>
 * Методы независимы друг от друга и могут использоваться отдельно.
 */
public class PartitionUtils {

    private static int mask;

    /**
     * Определяет номер инстанса приложения для заданного счета.
     * <p>
     * Использует хеширование для равномерного распределения счетов по инстансам.
     * Один и тот же UUID всегда маршрутизируется на один и тот же инстанс.
     *
     * @param accountId      UUID счета
     * @param totalInstances количество инстансов приложения (должно быть > 0)
     * @return номер инстанса от 0 до totalInstances-1
     * @throws IllegalArgumentException если параметры некорректны
     */
    public static int instanceNumberMurmurHash3(UUID accountId, int totalInstances) {
        if (totalInstances <= 0) {
            throw new IllegalArgumentException("Total instances must be positive");
        }
        if (accountId == null) {
            throw new IllegalArgumentException("Account ID cannot be null");
        }

        // Используем least significant bits для хеширования
        long hash = accountId.getLeastSignificantBits();

        // MurmurHash3 финализация для равномерного распределения
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);

        // Приводим к положительному и берем остаток
        return (int) ((hash & Long.MAX_VALUE) % totalInstances);
    }

    /**
     * Определяет номер партиции для заданного счета.
     * <p>
     * Использует хеширование для равномерного распределения счетов по партициям.
     * Один и тот же UUID всегда маршрутизируется в одну и ту же партицию.
     * <p>
     * Метод не зависит от количества инстансов - работает только с партициями.
     *
     * @param accountId       UUID счета
     * @param totalPartitions количество партиций (должно быть > 0)
     * @return номер партиции от 0 до totalPartitions-1
     * @throws IllegalArgumentException если параметры некорректны
     */
    public static int partitionNumberMurmurHash3(UUID accountId, int totalPartitions) {
        if (totalPartitions <= 0) {
            throw new IllegalArgumentException("Total partitions must be positive");
        }
        if (accountId == null) {
            throw new IllegalArgumentException("Account ID cannot be null");
        }

        // Используем most significant bits для хеширования
        // (чтобы иметь независимое распределение от getInstanceNumber)
        long hash = accountId.getMostSignificantBits();

        // MurmurHash3 финализация
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);

        // Приводим к положительному и берем остаток
        return (int) ((hash & Long.MAX_VALUE) % totalPartitions);
    }

    public static int partitionNumber(UUID accountId, int totalPartitions) {
        long msb = accountId.getMostSignificantBits();
        long lsb = accountId.getLeastSignificantBits();

        long hash = msb ^ lsb;
        hash ^= (hash >>> 32);

        // Проверяем power of 2
        if (Integer.bitCount(totalPartitions) == 1) {
            // Fast path: bit mask
            return (int) (hash & (totalPartitions - 1));
        } else {
            // Slow path: modulo
            return (int) (Long.remainderUnsigned(hash, totalPartitions));
        }
    }

    public static int instanceNumber(UUID accountId, int totalInstances) {
        long msb = accountId.getMostSignificantBits();
        long lsb = accountId.getLeastSignificantBits();

        long hash = msb ^ lsb;
        hash ^= (hash >>> 32);

        // Проверяем power of 2
        if (Integer.bitCount(totalInstances) == 1) {
            // Fast path: bit mask
            return (int) (hash & (totalInstances - 1));
        } else {
            // Slow path: modulo
            return (int) (Long.remainderUnsigned(hash, totalInstances));
        }
    }

    /**
     * Оптимизированная версия getInstanceNumber для степеней двойки.
     * Использует битовую маску вместо модуло - в ~3x быстрее.
     *
     * @param accountId      UUID счета
     * @param totalInstances количество инстансов (должно быть степенью 2)
     * @return номер инстанса от 0 до totalInstances-1
     * @throws IllegalArgumentException если totalInstances не является степенью 2
     */
    public static int instanceNumberPowerOfTwoMurmurHash3(UUID accountId, int totalInstances) {
        if (totalInstances <= 0 || (totalInstances & (totalInstances - 1)) != 0) {
            throw new IllegalArgumentException("Total instances must be a power of 2");
        }
        if (accountId == null) {
            throw new IllegalArgumentException("Account ID cannot be null");
        }

        long hash = accountId.getLeastSignificantBits();

        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);

        // Битовая маска вместо модуло
        return (int) (hash & (totalInstances - 1));
    }

    public static void totalPartitions(int totalPartitions) {
        if (Integer.bitCount(totalPartitions) != 1) {
            throw new IllegalArgumentException("Must be power of 2");
        }
        mask = totalPartitions - 1;
    }

    public static int partitionNumberPowerOfTwo(UUID accountId) {
        long msb = accountId.getMostSignificantBits();
        long lsb = accountId.getLeastSignificantBits();

        long hash = msb ^ lsb;
        hash ^= (hash >>> 32);

        return (int) (hash & mask);
    }

    public static int instanceNumberPowerOfTwo(UUID accountId) {
        long msb = accountId.getMostSignificantBits();
        long lsb = accountId.getLeastSignificantBits();

        long hash = msb ^ lsb;
        hash ^= (hash >>> 32);

        return (int) (hash & mask);
    }

    /**
     * Оптимизированная версия getPartitionNumber для степеней двойки.
     *
     * @param accountId       UUID счета
     * @param totalPartitions количество партиций (должно быть степенью 2)
     * @return номер партиции от 0 до totalPartitions-1
     * @throws IllegalArgumentException если totalPartitions не является степенью 2
     */
    public static int partitionNumberPowerOfTwoMurmurHash3(UUID accountId, int totalPartitions) {
        if (totalPartitions <= 0 || (totalPartitions & (totalPartitions - 1)) != 0) {
            throw new IllegalArgumentException("Total partitions must be a power of 2");
        }
        if (accountId == null) {
            throw new IllegalArgumentException("Account ID cannot be null");
        }

        long hash = accountId.getMostSignificantBits();

        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);

        // Битовая маска вместо модуло
        return (int) (hash & (totalPartitions - 1));
    }

    /**
     * Вспомогательный метод для одновременного получения инстанса и партиции.
     * Для удобства, если нужны оба значения.
     *
     * @param accountId       UUID счета
     * @param totalInstances  количество инстансов
     * @param totalPartitions количество партиций
     * @return массив [instanceNumber, partitionNumber]
     */
    public static int[] instanceAndPartition(UUID accountId, int totalInstances, int totalPartitions) {
        int instanceNumber = instanceNumber(accountId, totalInstances);
        int partitionNumber = partitionNumber(accountId, totalPartitions);
        return new int[]{instanceNumber, partitionNumber};
    }
}