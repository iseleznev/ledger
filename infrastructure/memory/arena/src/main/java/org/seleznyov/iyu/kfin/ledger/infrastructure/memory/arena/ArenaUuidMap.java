package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.UUID;

import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

public class ArenaUuidMap {

    private final Arena arena;
    private int capacity;
    private int mask;
    private MemorySegment memorySegment;

    private static final int MAP_ENTRY_SIZE = 32; // 8 + 8 + 8 + 4 + 4 (msb + lsb + value + state + padding)
    private static final int EMPTY = 0;
    private static final int OCCUPIED = 1;
    private static final int DELETED = 2;

    // Смещения в entry для лучшей читаемости
    private static final int MSB_OFFSET = 0;
    private static final int LSB_OFFSET = 8;
    private static final int VALUE_OFFSET = 16;
    private static final int STATE_OFFSET = 24;

    private int size;

    public ArenaUuidMap(Arena arena, int expectedCapacity) {
        // Находим ближайшую степень двойки
        this.capacity = nextPowerOfTwo(expectedCapacity * 2);
        this.mask = capacity - 1;
        this.arena = arena;
        this.memorySegment = arena.allocate((long) MAP_ENTRY_SIZE * capacity);
        this.size = 0;

        // Инициализируем все состояния как EMPTY
        initializeMemory();
    }

    private void initializeMemory() {
        for (int i = 0; i < capacity; i++) {
            setEntryState(i, EMPTY);
        }
    }

    public boolean put(UUID key, long value) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        if (size >= capacity * 0.75) {
            resize(capacity * 2);
            //throw new IllegalStateException("Hash table is too full, need resize");
        }

        // Счетчик для предотвращения бесконечного цикла

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;

        // Linear probing для поиска свободной ячейки
        int probeCount = 0;
        final int maxProbes = capacity;
//        while (true) {
        while (probeCount < maxProbes) {
            int state = getEntryState(index);

            if (state == EMPTY || state == DELETED) {
                // Найдена свободная ячейка
                setEntryMSB(index, msb);
                setEntryLSB(index, lsb);
                setEntryValue(index, value);
                setEntryState(index, OCCUPIED);
                size++;
                return true;
            } else if (state == OCCUPIED) {
                // Проверяем, не тот ли это ключ (update)
                if (getEntryMSB(index) == msb && getEntryLSB(index) == lsb) {
                    setEntryValue(index, value); // Обновляем значение
                    return false; // Не добавили новый элемент
                }
            }

            // Переходим к следующей ячейке (linear probing)
            index = (index + 1) & mask;
            probeCount++;
        }

        throw new IllegalStateException("Unable to find slot after " + maxProbes + " probes");

    }

    public long get(UUID key) {
        if (key == null) {
            return -1;
        }

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;

        // Linear probing для поиска
//        while (true) {
        int probeCount = 0;
        final int maxProbes = capacity;

        while (probeCount < maxProbes) {
            int state = getEntryState(index);

            if (state == EMPTY) {
                return -1; // Элемент не найден
            }

            if (state == OCCUPIED) {
                // Сравниваем ключи
                if (getEntryMSB(index) == msb && getEntryLSB(index) == lsb) {
                    return getEntryValue(index);
                }
            }

            // Продолжаем поиск (deleted ячейки пропускаем)
            index = (index + 1) & mask;
            probeCount++;
        }
        return -1;
    }

    public boolean remove(UUID key) {
        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;

        while (true) {
            int state = getEntryState(index);

            if (state == EMPTY) {
                return false; // Элемент не найден
            }

            if (state == OCCUPIED) {
                if (getEntryMSB(index) == msb && getEntryLSB(index) == lsb) {
                    setEntryState(index, DELETED);
                    size--;
                    return true;
                }
            }

            index = (index + 1) & mask;
        }
    }

//    private static int hash(long msb, long lsb) {
//        long hash = msb ^ lsb;
//        hash ^= hash >>> 33;
//        hash *= 0xff51afd7ed558ccdL;
//        hash ^= hash >>> 33;
//        hash *= 0xc4ceb9fe1a85ec53L;
//        hash ^= hash >>> 33;
//        return (int) hash;
//    }

    private static int hash(long msb, long lsb) {
        long hash = 0xcbf29ce484222325L;

        // MSB processing (unrolled)
        hash ^= msb & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 8) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 16) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 24) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 32) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 40) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 48) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (msb >>> 56) & 0xFF; hash *= 0x100000001b3L;

        // LSB processing (unrolled)
        hash ^= lsb & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 8) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 16) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 24) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 32) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 40) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 48) & 0xFF; hash *= 0x100000001b3L;
        hash ^= (lsb >>> 56) & 0xFF; hash *= 0x100000001b3L;

        return (int) (hash ^ (hash >>> 32));
    }

    private static int hash(UUID key) {
        return hash(key.getMostSignificantBits(), key.getLeastSignificantBits());
    }

    // Вычисление адреса entry
    private long entryOffset(int index) {
        return (long) index * MAP_ENTRY_SIZE;
    }

    // Чтение/запись MSB (старшие 64 бита UUID)
    private long getEntryMSB(int index) {
        return memorySegment.get(JAVA_LONG, entryOffset(index));
    }

    private void setEntryMSB(int index, long msb) {
        memorySegment.set(JAVA_LONG, entryOffset(index), msb);
    }

    // Чтение/запись LSB (младшие 64 бита UUID)
    private long getEntryLSB(int index) {
        return memorySegment.get(JAVA_LONG, entryOffset(index) + 8);
    }

    private void setEntryLSB(int index, long lsb) {
        memorySegment.set(JAVA_LONG, entryOffset(index) + 8, lsb);
    }

    // Чтение/запись значения (индекс в массиве)
    private long getEntryValue(int index) {
        return memorySegment.get(JAVA_LONG, entryOffset(index) + 16);
    }

    private void setEntryValue(int index, long value) {
        memorySegment.set(JAVA_LONG, entryOffset(index) + 16, value);
    }

    // Чтение/запись состояния
    private int getEntryState(int index) {
        return memorySegment.get(JAVA_INT, entryOffset(index) + 24);
    }

    private void setEntryState(int index, int state) {
        memorySegment.set(JAVA_INT, entryOffset(index) + 24, state);
    }

    private static int nextPowerOfTwo(int n) {
        if (n <= 1) return 1;

        // Заполняем все биты справа от старшего
        n--;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;

        return n + 1;
    }

    public void resize(int newCapacity) {
        if (newCapacity <= capacity) {
            throw new IllegalArgumentException(
                String.format("New capacity (%d) must be greater than current (%d)",
                    newCapacity, capacity));
        }

        int actualNewCapacity = nextPowerOfTwo(newCapacity);

        // Сохраняем старые данные
        MemorySegment oldMemory = memorySegment;
        int oldCapacity = capacity;
        int oldSize = size;

        // Выделяем новую память
        this.capacity = actualNewCapacity;
        this.mask = capacity - 1;
        this.memorySegment = arena.allocate((long) MAP_ENTRY_SIZE * capacity);
        this.size = 0;

        // Инициализируем новую память
        initializeMemory();

        try {
            // Перемещаем все элементы из старой таблицы в новую
            rehashAllEntries(oldMemory, oldCapacity);

            if (size != oldSize) {
                throw new IllegalStateException(
                    String.format("Size mismatch after resize: expected %d, got %d",
                        oldSize, size));
            }

        } catch (Exception e) {
            // В случае ошибки откатываем изменения
            this.memorySegment = oldMemory;
            this.capacity = oldCapacity;
            this.mask = oldCapacity - 1;
            this.size = oldSize;
            throw new RuntimeException("Failed to resize hash table", e);
        }
    }

    /**
     * Принудительный resize до конкретного размера
     */
    public void resizeToExact(int exactCapacity) {
        if (!isPowerOfTwo(exactCapacity)) {
            throw new IllegalArgumentException("Capacity must be power of two: " + exactCapacity);
        }
        resize(exactCapacity);
    }

    /**
     * Умный resize основанный на прогнозе роста
     */
    public void resizeForExpectedSize(int expectedElements) {
        int requiredCapacity = (int) Math.ceil(expectedElements / 0.75); // Учитываем load factor
        int targetCapacity = nextPowerOfTwo(requiredCapacity);

        if (targetCapacity > capacity) {
            resize(targetCapacity);
        }
    }

    private void rehashAllEntries(MemorySegment oldMemory, int oldCapacity) {
        for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
            int state = oldMemory.get(JAVA_INT,
                (long) oldIndex * MAP_ENTRY_SIZE + STATE_OFFSET);

            if (state == OCCUPIED) {
                long msb = oldMemory.get(JAVA_LONG,
                    (long) oldIndex * MAP_ENTRY_SIZE + MSB_OFFSET);
                long lsb = oldMemory.get(JAVA_LONG,
                    (long) oldIndex * MAP_ENTRY_SIZE + LSB_OFFSET);
                long value = oldMemory.get(JAVA_LONG,
                    (long) oldIndex * MAP_ENTRY_SIZE + VALUE_OFFSET);

                // Вставляем в новую таблицу
                rehashSingleEntry(msb, lsb, value);
            }
        }
    }

    private void setEntry(int index, long msb, long lsb, long value, int state) {
        long offset = entryOffset(index);
        memorySegment.set(JAVA_LONG, offset + MSB_OFFSET, msb);
        memorySegment.set(JAVA_LONG, offset + LSB_OFFSET, lsb);
        memorySegment.set(JAVA_LONG, offset + VALUE_OFFSET, value);
        memorySegment.set(JAVA_INT, offset + STATE_OFFSET, state);
    }

    private void rehashSingleEntry(long msb, long lsb, long value) {
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;

        // Linear probing для поиска свободной ячейки
        while (true) {
            int state = getEntryState(index);

            if (state == EMPTY) {
                setEntry(index, msb, lsb, value, OCCUPIED);
                size++;
                return;
            }

            // В новой таблице не должно быть коллизий, но на всякий случай
            index = (index + 1) & mask;
        }
    }

    private static boolean isPowerOfTwo(int n) {
        return n > 0 && (n & (n - 1)) == 0;
    }

    /**
     * Проверяет, нужен ли resize на основе текущего load factor
     */
    public boolean needsResize() {
        return loadFactor() > 0.75;
    }

    /**
     * Проверяет, нужен ли resize для добавления указанного количества элементов
     */
    public boolean needsResizeFor(int additionalElements) {
        return (size + additionalElements) > capacity * 0.75;
    }

    /**
     * Рекомендуемый размер для resize
     */
    public int recommendedResizeCapacity() {
        if (!needsResize()) {
            return capacity;
        }
        return nextPowerOfTwo(capacity * 2);
    }

    public double loadFactor() {
        return (double) size / capacity;
    }
}
