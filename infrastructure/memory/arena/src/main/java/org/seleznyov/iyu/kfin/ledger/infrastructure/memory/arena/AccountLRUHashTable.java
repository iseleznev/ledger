package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.UUID;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class AccountLRUHashTable {

    private final Arena arena;
    private int capacity;
    private int mask;
    private MemorySegment memorySegment;
    private MemorySegment timelineBufferMemorySegment;

    private static final int MAP_ENTRY_SIZE = 64; // 8 + 8 + 8 + 8 + 8 + 8 + 8 + 4 + 4
    private static final int EMPTY = 0;
    private static final int OCCUPIED = 1;
    private static final int DELETED = 2;

    // Смещения в entry
    private static final int MSB_OFFSET = 0;
    private static final int LSB_OFFSET = 8;
    private static final int TIMELINE_BUFFER_INDEX_OFFSET = 16;
    private static final int ACCOUNT_STATE_HASH_TABLE_OFFSET = 24;
    private static final int BALANCE_OFFSET = 32;
    private static final int ORDINAL_OFFSET = 40;
    private static final int COUNT_OFFSET = 48;
    private static final int STATE_OFFSET = 56;

    // Robin Hood параметры
    private static final int MAX_PSL = 16; // Максимальный Probe Sequence Length
    private static final double LOAD_FACTOR = 0.85; // Агрессивный для экономии памяти
    private static final int PREALLOC_CHUNK = 1_048_576; // 1M элементов

    private int size;
    private long totalProbes; // Статистика для мониторинга
    private int maxPsl; // Максимальный PSL в таблице

    public AccountLRUHashTable(Arena arena, int expectedCapacity) {
        this.capacity = Math.max(nextPowerOfTwo(expectedCapacity * 2), PREALLOC_CHUNK);
        this.mask = capacity - 1;
        this.arena = arena;
        this.memorySegment = arena.allocate((long) MAP_ENTRY_SIZE * capacity);
        this.timelineBufferMemorySegment = arena.allocate(16L * capacity * 5);
        this.size = 0;
        this.totalProbes = 0;
        this.maxPsl = 0;

        initializeMemory();
    }

    private void initializeMemory() {
        // Быстрая инициализация через батчи
        long totalBytes = (long) capacity * MAP_ENTRY_SIZE;
        for (long offset = 0; offset < totalBytes; offset += MAP_ENTRY_SIZE) {
            memorySegment.set(JAVA_INT, offset + STATE_OFFSET, EMPTY);
        }
    }

    /**
     * Robin Hood put - основной метод вставки
     */
    public long put(UUID key, long balance, long ordinal, long count) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (size >= capacity * LOAD_FACTOR) {
            expandCapacity();
        }

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int idealIndex = hashCode & mask;

        // Элемент для вставки
        long insertMsb = msb;
        long insertLsb = lsb;
        long insertBalance = balance;
        long insertOrdinal = ordinal;
        long insertCount = count;

        int currentIndex = idealIndex;
        int currentPsl = 0;

        while (currentPsl <= MAX_PSL) {
            long currentOffset = (long) currentIndex * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, currentOffset + STATE_OFFSET);
            int recentCount = memorySegment.get(JAVA_INT, currentOffset + STATE_OFFSET);

            if (state == EMPTY || state == DELETED || ) {
                // Вставляем элемент - inline операции для скорости
                memorySegment.set(JAVA_LONG, currentOffset + MSB_OFFSET, insertMsb);
                memorySegment.set(JAVA_LONG, currentOffset + LSB_OFFSET, insertLsb);
                memorySegment.set(JAVA_LONG, currentOffset + BALANCE_OFFSET, insertBalance);
                memorySegment.set(JAVA_LONG, currentOffset + ORDINAL_OFFSET, insertOrdinal);
                memorySegment.set(JAVA_LONG, currentOffset + COUNT_OFFSET, insertCount);
                memorySegment.set(JAVA_INT, currentOffset + STATE_OFFSET, OCCUPIED);

                if (state == EMPTY) size++;
                if (currentPsl > maxPsl) maxPsl = currentPsl;
                totalProbes += currentPsl + 1;
                return currentOffset;
            }

            if (state == OCCUPIED) {
                long existingMsb = memorySegment.get(JAVA_LONG, currentOffset + MSB_OFFSET);
                long existingLsb = memorySegment.get(JAVA_LONG, currentOffset + LSB_OFFSET);

                // Проверяем на обновление существующего ключа
                if (existingMsb == insertMsb && existingLsb == insertLsb) {
                    // Обновление - inline операции
                    memorySegment.set(JAVA_LONG, currentOffset + BALANCE_OFFSET, insertBalance);
                    memorySegment.set(JAVA_LONG, currentOffset + ORDINAL_OFFSET, insertOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + COUNT_OFFSET, insertCount);
                    return currentOffset; // Обновление, не вставка
                }

                // Robin Hood логика: вычисляем PSL существующего элемента
                int existingHash = hash(existingMsb, existingLsb);
                int existingIdeal = existingHash & mask;
                int existingPsl = (currentIndex - existingIdeal + capacity) & mask;

                // Если наш PSL больше - вытесняем существующий элемент
                if (currentPsl > existingPsl) {
                    // Сохраняем существующий элемент для дальнейшей вставки
                    long tempBalance = memorySegment.get(JAVA_LONG, currentOffset + BALANCE_OFFSET);
                    long tempOrdinal = memorySegment.get(JAVA_LONG, currentOffset + ORDINAL_OFFSET);
                    long tempCount = memorySegment.get(JAVA_LONG, currentOffset + COUNT_OFFSET);

                    // Записываем наш элемент на место существующего
                    memorySegment.set(JAVA_LONG, currentOffset + MSB_OFFSET, insertMsb);
                    memorySegment.set(JAVA_LONG, currentOffset + LSB_OFFSET, insertLsb);
                    memorySegment.set(JAVA_LONG, currentOffset + BALANCE_OFFSET, insertBalance);
                    memorySegment.set(JAVA_LONG, currentOffset + ORDINAL_OFFSET, insertOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + COUNT_OFFSET, insertCount);

                    // Продолжаем с вытесненным элементом
                    insertMsb = existingMsb;
                    insertLsb = existingLsb;
                    insertBalance = tempBalance;
                    insertOrdinal = tempOrdinal;
                    insertCount = tempCount;
                    currentPsl = existingPsl;
                }
            }

            currentIndex = (currentIndex + 1) & mask;
            currentPsl++;
        }

        // Если PSL превысил лимит - расширяем таблицу и пытаемся снова
        expandCapacity();
        return put(key, balance, ordinal, count);
    }

    public void update(long currentOffset, long balance, long ordinal, long count) {
        memorySegment.set(JAVA_LONG, currentOffset + BALANCE_OFFSET, balance);
        memorySegment.set(JAVA_LONG, currentOffset + ORDINAL_OFFSET, ordinal);
        memorySegment.set(JAVA_LONG, currentOffset + COUNT_OFFSET, count);
    }

    public long getOffset(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, MAX_PSL);

        while (psl <= maxSearch) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) return -1;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    return offset;
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }


    /**
     * Robin Hood поиск - оптимизированный для скорости
     */
    public long getBalance(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        // Ограничиваем поиск максимальным PSL в таблице + небольшой буфер
        int maxSearch = Math.min(maxPsl + 2, MAX_PSL);

        while (psl <= maxSearch) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) {
                return -1; // Элемент не найден
            }

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    return memorySegment.get(JAVA_LONG, offset + BALANCE_OFFSET);
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public long getBalance(long offset) {
        return memorySegment.get(JAVA_LONG, offset + BALANCE_OFFSET);
    }

    public long getOrdinal(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, MAX_PSL);

        while (psl <= maxSearch) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) return -1;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    return memorySegment.get(JAVA_LONG, offset + ORDINAL_OFFSET);
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public long getOrdinal(long offset) {
        return memorySegment.get(JAVA_LONG, offset + ORDINAL_OFFSET);
    }

    public long getCount(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, MAX_PSL);

        while (psl <= maxSearch) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) return -1;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    return memorySegment.get(JAVA_LONG, offset + COUNT_OFFSET);
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public long getCount(long offset) {
        return memorySegment.get(JAVA_LONG, offset + COUNT_OFFSET);
    }


    /**
     * Специализированные методы для отдельных полей
     */
    public long putBalance(UUID key, long value) {
        // Получаем существующие значения одним поиском
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        // Пытаемся найти и обновить существующий элемент
        while (psl <= maxPsl + 2) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) break;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    // Обновляем только balance
                    memorySegment.set(JAVA_LONG, offset + BALANCE_OFFSET, value);
                    return offset; // Обновление
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        // Элемент не найден - создаем новый с дефолтными значениями
        return put(key, value, 0L, 0L);
    }

    public long putOrdinal(UUID key, long value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        while (psl <= maxPsl + 2) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) break;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    memorySegment.set(JAVA_LONG, offset + ORDINAL_OFFSET, value);
                    return offset;
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return put(key, 0L, value, 0L);
    }

    public long putCount(UUID key, long value) {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        while (psl <= maxPsl + 2) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) break;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    memorySegment.set(JAVA_LONG, offset + COUNT_OFFSET, value);
                    return offset;
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return put(key, 0L, 0L, value);
    }

    public boolean remove(UUID key) {
        if (key == null) return false;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        while (psl <= maxPsl + 2) {
            long offset = (long) index * MAP_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);

            if (state == EMPTY) return false;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    memorySegment.set(JAVA_INT, offset + STATE_OFFSET, DELETED);
                    size--;
                    return true;
                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return false;
    }

    /**
     * Расширение таблицы с preallocation чанками
     */
    private void expandCapacity() {
        int newCapacity = Math.max(capacity * 2,
            nextPowerOfTwo((int) (size / LOAD_FACTOR) + PREALLOC_CHUNK));

        resize(newCapacity);
    }

    private void resize(int newCapacity) {
        if (newCapacity <= capacity) {
            throw new IllegalArgumentException("New capacity must be greater than current");
        }

        // Сохраняем старые данные
        MemorySegment oldMemory = memorySegment;
        int oldCapacity = capacity;
        int oldSize = size;

        // Выделяем новую память
        this.capacity = nextPowerOfTwo(newCapacity);
        this.mask = capacity - 1;
        this.memorySegment = arena.allocate((long) MAP_ENTRY_SIZE * capacity);
        this.size = 0;
        this.maxPsl = 0;

        initializeMemory();

        try {
            // Полный rehash с Robin Hood
            for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
                long oldOffset = (long) oldIndex * MAP_ENTRY_SIZE;
                int state = oldMemory.get(JAVA_INT, oldOffset + STATE_OFFSET);

                if (state == OCCUPIED) {
                    long msb = oldMemory.get(JAVA_LONG, oldOffset + MSB_OFFSET);
                    long lsb = oldMemory.get(JAVA_LONG, oldOffset + LSB_OFFSET);
                    long balance = oldMemory.get(JAVA_LONG, oldOffset + BALANCE_OFFSET);
                    long ordinal = oldMemory.get(JAVA_LONG, oldOffset + ORDINAL_OFFSET);
                    long count = oldMemory.get(JAVA_LONG, oldOffset + COUNT_OFFSET);

                    put(new UUID(msb, lsb), balance, ordinal, count);
                }
            }

            if (size != oldSize) {
                throw new IllegalStateException("Size mismatch after resize");
            }

        } catch (Exception e) {
            // Откат при ошибке
            this.memorySegment = oldMemory;
            this.capacity = oldCapacity;
            this.mask = oldCapacity - 1;
            this.size = oldSize;
            throw new RuntimeException("Failed to resize hash table", e);
        }
    }

    /**
     * Optimized FNV-1a hash для UUID
     */
    private static int hash(long msb, long lsb) {
        long hash = 0xcbf29ce484222325L;

        // MSB processing - развернуто для скорости
        hash ^= msb & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 8) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 16) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 24) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 32) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 40) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 48) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (msb >>> 56) & 0xFF;
        hash *= 0x100000001b3L;

        // LSB processing
        hash ^= lsb & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 8) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 16) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 24) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 32) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 40) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 48) & 0xFF;
        hash *= 0x100000001b3L;
        hash ^= (lsb >>> 56) & 0xFF;
        hash *= 0x100000001b3L;

        return (int) (hash ^ (hash >>> 32));
    }

    private static int nextPowerOfTwo(int n) {
        if (n <= 1) return 1;
        n--;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }

    // Утилиты и статистика
    public double loadFactor() {
        return (double) size / capacity;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return capacity;
    }

    public double averageProbeLength() {
        return size > 0 ? (double) totalProbes / size : 0.0;
    }

    public int maxProbeLength() {
        return maxPsl;
    }

    /**
     * Статистика для мониторинга производительности
     */
    public String getStats() {
        return String.format(
            "AccountStateMap Stats: size=%d, capacity=%d, loadFactor=%.2f, avgPSL=%.2f, maxPSL=%d",
            size, capacity, loadFactor(), averageProbeLength(), maxPsl
        );
    }

    /**
     * Проверка необходимости cleanup (удаление DELETED ячеек)
     */
    public boolean needsCleanup() {
        // Если много DELETED ячеек и низкий load factor
        return loadFactor() < 0.5 && maxPsl > 8;
    }

    /**
     * Cleanup - удаление DELETED ячеек через rebuild
     */
    public void cleanup() {
        if (!needsCleanup()) return;

        int optimalCapacity = Math.max(nextPowerOfTwo((int) (size / LOAD_FACTOR)), PREALLOC_CHUNK);
        resize(optimalCapacity);
    }
}