package org.seleznyov.iyu.kfin.ledgerservice.core.hashtable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.UUID;

import static java.lang.foreign.ValueLayout.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.AccountPartitionHashTableConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.OrdinalUtils.nextOrdinal;

public class AccountPartitionLazyResizeHashTable implements AccountPartitionHashTable {

    private final static int MAX_EXPANSION_ATTEMPTS = 3;

    private final static int MIGRATION_BATCH_SIZE = 16; // Размер порции для миграции
    private final static short MIGRATED = 99; // Маркер мигрированных элементов

    // Поля для хранения старой таблицы во время миграции
    private MemorySegment oldMemorySegment = null;
    private int oldCapacity = 0;
    private int oldMask = 0;
    private int migrationIndex = 0; // Текущая позиция в процессе миграции
    private boolean isRehashing = false; // Флаг активной миграции

    private final Arena arena;
    private int capacity;
    private int mask;
    private MemorySegment memorySegment;

    private final int maxAcceptedPsl;
    private int size;
    private long totalProbes; // Статистика для мониторинга
    private int maxPsl; // Максимальный PSL в таблице
    private long currentTableOrdinal = 0;
    private long committedTableOrdinal = 0;

    public AccountPartitionLazyResizeHashTable(Arena arena, int expectedCapacity, int maxAcceptedPsl) {
        this.capacity = Math.max(nextPowerOfTwo(expectedCapacity * 2), HASH_TABLE_PREALLOC_CHUNK);
        this.mask = capacity - 1;
        this.arena = arena;
        this.memorySegment = arena.allocate((long) HASH_TABLE_ENTRY_SIZE * capacity);
        this.size = 0;
        this.totalProbes = 0;
        this.maxPsl = 0;
        this.maxAcceptedPsl = expectedCapacity;

        initializeMemory();
    }

    @Override
    public MemorySegment memorySegment() {
        return this.memorySegment;
    }

    private void initializeMemory() {
        // Быстрая инициализация через батчи
        long totalBytes = (long) capacity * HASH_TABLE_ENTRY_SIZE;
        for (long offset = 0; offset < totalBytes; offset += HASH_TABLE_ENTRY_SIZE) {
            memorySegment.set(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET, EMPTY);
        }
    }

    /**
     * Инкрементальная миграция - вызывается при каждой операции
     */
    private void rehashingMigrationOnDemand() {
        int migrated = 0;
        int startIndex = migrationIndex;

        // Мигрируем batch элементов
        while (migrated < MIGRATION_BATCH_SIZE && migrationIndex < oldCapacity) {
            long oldOffset = (long) migrationIndex * HASH_TABLE_ENTRY_SIZE;
            short state = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET);

            if (state == OCCUPIED) {
                // Читаем элемент из старой таблицы
                long msb = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                long lsb = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                long balance = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                long ordinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                long count = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                long stagedAmount = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                long tableOrdinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ORDINAL_OFFSET);
                short accountRate = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                // Вставляем в новую таблицу напрямую (без проверки на resize)
                putInternal(msb, lsb, balance, ordinal, count, stagedAmount, tableOrdinal, accountRate);

                // Помечаем как мигрированный
                oldMemorySegment.set(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET, MIGRATED);
            }

            migrationIndex++;
            migrated++;
        }

        // Завершили миграцию
        if (migrationIndex >= oldCapacity) {
            oldMemorySegment = null;
            oldCapacity = 0;
            oldMask = 0;
            migrationIndex = 0;
            isRehashing = false;
        }
    }

    /**
     * Начало ленивого resize - только выделение памяти
     */
    private void startLazyResize() {
        if (isRehashing) {
            // Уже в процессе rehashing, просто продолжаем миграцию
            return;
        }

        int newCapacity = Math.max(capacity * 2,
            nextPowerOfTwo((int) (size / HASH_TABLE_LOAD_FACTOR) + HASH_TABLE_PREALLOC_CHUNK));

        if (newCapacity <= capacity) {
            throw new IllegalArgumentException("New capacity must be greater than current");
        }

        // Сохраняем старую таблицу
        oldMemorySegment = memorySegment;
        oldCapacity = capacity;
        oldMask = mask;

        // Выделяем новую память
        this.capacity = nextPowerOfTwo(newCapacity);
        this.mask = capacity - 1;
        this.memorySegment = arena.allocate((long) HASH_TABLE_ENTRY_SIZE * capacity);
        this.maxPsl = 0;

        // НЕ изменяем size - он будет актуален для обеих таблиц

        initializeMemory();

        // Запускаем процесс миграции
        isRehashing = true;
        migrationIndex = 0;
    }

    /**
     * Внутренний put без проверки на необходимость resize
     */
    private long putInternal(long msb, long lsb, long balance, long ordinal, long count,
                             long stagedAmount, long tableOrdinal, short accountRate) {
        int hashCode = hash(msb, lsb);
        int idealIndex = hashCode & mask;

        long insertMsb = msb;
        long insertLsb = lsb;
        long insertBalance = balance;
        long insertOrdinal = ordinal;
        long insertStagedAmount = stagedAmount;
        long insertTableOrdinal = tableOrdinal;
        long insertCount = count;
        short insertAccountRate = accountRate;

        int currentIndex = idealIndex;
        int currentPsl = 0;

        while (currentPsl <= maxAcceptedPsl) {
            long currentOffset = (long) currentIndex * HASH_TABLE_ENTRY_SIZE;
            short state = memorySegment.get(JAVA_SHORT, currentOffset + HASH_TABLE_STATE_OFFSET);

            if (state == EMPTY || state == DELETED) {
                // Вставляем элемент
                memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_STATE_OFFSET, OCCUPIED);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET, insertMsb);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET, insertLsb);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertTableOrdinal);
                memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);
                memorySegment.set(JAVA_INT, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);

                if (currentPsl > maxPsl) maxPsl = currentPsl;
                totalProbes += currentPsl + 1;
                return currentOffset;
            }

            if (state == OCCUPIED) {
                long existingMsb = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                long existingLsb = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);

                // Обновление существующего ключа
                if (existingMsb == insertMsb && existingLsb == insertLsb) {
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertTableOrdinal);
                    memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);
                    memorySegment.set(JAVA_INT, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);
                    return currentOffset;
                }

                // Robin Hood логика
                int existingHash = hash(existingMsb, existingLsb);
                int existingIdeal = existingHash & mask;
                int existingPsl = (currentIndex - existingIdeal + capacity) & mask;

                if (currentPsl > existingPsl) {
                    // Swap
                    long tempBalance = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                    long tempOrdinal = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    long tempCount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                    long tempTableOrdinal = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET);
                    long tempStagedAmount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                    short tempAccountRate = memorySegment.get(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET, insertMsb);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET, insertLsb);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertTableOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                    memorySegment.set(JAVA_INT, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);
                    memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);

                    insertMsb = existingMsb;
                    insertLsb = existingLsb;
                    insertBalance = tempBalance;
                    insertOrdinal = tempOrdinal;
                    insertCount = tempCount;
                    insertStagedAmount = tempStagedAmount;
                    insertTableOrdinal = tempTableOrdinal;
                    insertAccountRate = tempAccountRate;
                    currentPsl = existingPsl;
                }
            }

            currentIndex = (currentIndex + 1) & mask;
            currentPsl++;
        }

        throw new IllegalStateException("Unable to insert element, PSL exceeded: " + currentPsl);
    }

    public void copyAccountState(MemorySegment targetMemorySegment, long accountOffset, long targetOffset) {
        MemorySegment.copy(memorySegment, accountOffset, targetMemorySegment, targetOffset, HASH_TABLE_ENTRY_SIZE);
    }

    public void committedTableOrdinal(long tableOrdinal) {
        this.committedTableOrdinal = tableOrdinal;
    }

    public void currentTableOrdinal(long tableOrdinal) {
        this.currentTableOrdinal = tableOrdinal;
    }

    public long currentTableOrdinalForward() {
        this.currentTableOrdinal++;
        return this.currentTableOrdinal;
    }

    public long currentTableOrdinal() {
        return currentTableOrdinal;
    }

    public long committedTableOrdinal() {
        return committedTableOrdinal;
    }

    /**
     * Поиск элемента в старой таблице
     */
    private long findInOldTable(long msb, long lsb) {
        if (oldMemorySegment == null) return -1;

        int hashCode = hash(msb, lsb);
        int index = hashCode & oldMask;
        int psl = 0;

        while (psl <= maxAcceptedPsl) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            short state = oldMemorySegment.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);

            if (state == EMPTY) return -1;
            if (state == MIGRATED) {
                // Элемент уже мигрирован
                return -1;
            }

            if (state == OCCUPIED) {
                long entryMsb = oldMemorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                long entryLsb = oldMemorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                if (entryMsb == msb && entryLsb == lsb) {
                    return offset;
                }
            }

            index = (index + 1) & oldMask;
            psl++;
        }

        return -1;
    }

    /**
     * Поиск offset в конкретной таблице
     */
    private long getOffsetInTable(MemorySegment memory, int tableMask, long msb, long lsb) {
        int hashCode = hash(msb, lsb);
        int index = hashCode & tableMask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, maxAcceptedPsl);

        while (psl <= maxSearch) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            short state = memory.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);

            if (state == EMPTY) return -1;

            if (state == OCCUPIED) {
                long entryMsb = memory.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                long entryLsb = memory.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                if (entryMsb == msb && entryLsb == lsb) {
                    return offset;
                }
            }

            index = (index + 1) & tableMask;
            psl++;
        }

        return -1;
    }

    /**
     * Get с поддержкой двух таблиц
     */
    public long getOffset(UUID accountId) {
        if (accountId == null) return -1;

        // Инкрементальная миграция
        if (isRehashing) {
            rehashingMigrationOnDemand();
        }

        long accountIdMsb = accountId.getMostSignificantBits();
        long accountIdLsb = accountId.getLeastSignificantBits();

        // Сначала ищем в новой таблице
        long offset = getOffsetInTable(memorySegment, mask, accountIdMsb, accountIdLsb);
        if (offset >= 0) return offset;

        // Если в процессе rehashing, проверяем старую таблицу
        if (isRehashing && oldMemorySegment != null) {
            long oldOffset = findInOldTable(accountIdMsb, accountIdLsb);
            if (oldOffset >= 0) {
                // Найден в старой таблице - мигрируем немедленно
                short state = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET);
                if (state == OCCUPIED) {
                    // Читаем все данные
                    long balance = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                    long ordinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    long count = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                    long stagedAmount = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                    long tableOrdinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ORDINAL_OFFSET);
                    short accountRate = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                    // Помечаем как мигрированный
                    oldMemorySegment.set(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET, MIGRATED);

                    // Вставляем в новую таблицу
                    return putInternal(accountIdMsb, accountIdLsb, balance, ordinal, count,
                        stagedAmount, tableOrdinal, accountRate);
                }
            }
        }

        return -1;
    }

    /**
     * Основной метод put с проверкой на resize и инкрементальной миграцией
     */
    public long put(UUID accountId, long balance, long ordinal, long count) {
        if (accountId == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        // Инкрементальная миграция при каждой операции
        if (isRehashing) {
            rehashingMigrationOnDemand();
        }

        // Проверка на необходимость resize
        if (size >= capacity * HASH_TABLE_LOAD_FACTOR && !isRehashing) {
            startLazyResize();
        }

        long accountIdMsb = accountId.getMostSignificantBits();
        long accountIdLsb = accountId.getLeastSignificantBits();

        // Если в процессе rehashing, проверяем старую таблицу
        if (isRehashing) {
            long oldOffset = findInOldTable(accountIdMsb, accountIdLsb);
            if (oldOffset >= 0) {
                // Элемент еще в старой таблице - мигрируем его прямо сейчас
                short state = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET);
                if (state == OCCUPIED) {
                    // Копируем все данные
                    long oldStagedAmount = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                    long oldTableOrdinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ORDINAL_OFFSET);
                    short oldAccountRate = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                    // Помечаем как мигрированный
                    oldMemorySegment.set(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET, MIGRATED);

                    // Вставляем с новыми значениями
                    long newOffset = putInternal(accountIdMsb, accountIdLsb, balance, ordinal, count,
                        oldStagedAmount, oldTableOrdinal, oldAccountRate);
                    return newOffset;
                }
            }
        }

        // Вставляем в текущую (новую) таблицу
        long offset = putInternal(accountIdMsb, accountIdLsb, balance, ordinal, count,
            0L, this.currentTableOrdinal, RATE_COLD_ACCOUNT);

        // Увеличиваем size только при новой вставке (не обновлении)
        long existingOffset = getOffsetInTable(memorySegment, mask, accountIdMsb, accountIdLsb);
        if (existingOffset < 0 || existingOffset == offset) {
            size++;
        }

        return offset;
    }

    public long put(UUID accountId) {
        if (accountId == null) {
            throw new IllegalArgumentException("Account Id cannot be null");
        }

        // Инкрементальная миграция при каждой операции
        if (isRehashing) {
            rehashingMigrationOnDemand();
        }

        // Проверка на необходимость resize
        if (size >= capacity * HASH_TABLE_LOAD_FACTOR && !isRehashing) {
            startLazyResize();
        }

        long accountIdMsb = accountId.getMostSignificantBits();
        long accountIdLsb = accountId.getLeastSignificantBits();

        // Если в процессе rehashing, проверяем старую таблицу
        if (isRehashing) {
            long oldOffset = findInOldTable(accountIdMsb, accountIdLsb);
            if (oldOffset >= 0) {
                // Элемент еще в старой таблице - мигрируем его прямо сейчас
                short state = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET);
                if (state == OCCUPIED) {
                    // Копируем все данные
                    long oldBalance = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                    long oldOrdinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    long oldCount = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                    long oldStagedAmount = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                    long oldTableOrdinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ORDINAL_OFFSET);
                    short oldAccountRate = oldMemorySegment.get(JAVA_SHORT, oldOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                    // Помечаем как мигрированный
                    oldMemorySegment.set(JAVA_SHORT, oldOffset + HASH_TABLE_STATE_OFFSET, MIGRATED);

                    // Вставляем с новыми значениями
                    long newOffset = putInternal(accountIdMsb, accountIdLsb, oldBalance, oldOrdinal, oldCount,
                        oldStagedAmount, oldTableOrdinal, oldAccountRate);
                    return newOffset;
                }
            }
        }

        // Вставляем в текущую (новую) таблицу
        long offset = putInternal(accountIdMsb, accountIdLsb, 0, 0, 0,
            0L, this.currentTableOrdinal, RATE_COLD_ACCOUNT);

        // Увеличиваем size только при новой вставке (не обновлении)
        long existingOffset = getOffsetInTable(memorySegment, mask, accountIdMsb, accountIdLsb);
        if (existingOffset < 0 || existingOffset == offset) {
            size++;
        }

        return offset;
    }

    public void update(long currentOffset, long balance, long ordinal, long count) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, balance);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, ordinal);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, count);
//        memorySegment.set(JAVA_LONG, currentOffset + CURRENT_ORDINAL_SEQUENCE_OFFSET, this.currentOrdinalSequence);
    }

    public void updateAccountTableCurrentOrdinal(long currentOffset, long tableCurrentOrdinal) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, tableCurrentOrdinal);
    }

    public void updateAccountTableCurrentOrdinalForward(long currentOffset) {
        this.currentTableOrdinal++;
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, this.currentTableOrdinal);
    }

    public void updateStagedAmount(long currentOffset, long stagedAmount) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, stagedAmount);
    }

    public void increaseStagedAmount(long currentOffset, long increaseStagedAmount) {
        final long stagedAmount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, stagedAmount + increaseStagedAmount);
    }

    public long getStagedAmount(long currentOffset) {
        return memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
    }

    public void updateBalance(long currentOffset, long balance) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, balance);
    }

    public void increaseBalance(long currentOffset, long increaseAmount) {
        final long balance = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, balance + increaseAmount);
    }

    public void commitBalance(long currentOffset) {
        final long stagedAmount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
        final long balance = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, balance + stagedAmount);

        final long count = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, count + 1);
    }

    public void updateOrdinal(long currentOffset, long ordinal) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, ordinal);
    }

    public void updateCount(long currentOffset, long count) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, count);
    }

    /**
     * Robin Hood поиск - оптимизированный для скорости
     */
    public long getBalance(UUID key) {
        if (key == null) {
            return -1;
        }

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        // Ограничиваем поиск максимальным PSL в таблице + небольшой буфер
        int maxSearch = Math.min(maxPsl + 2, maxAcceptedPsl);

        while (psl <= maxSearch) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);
            int storedPsl = memorySegment.get(JAVA_INT, offset + HASH_TABLE_PSL_OFFSET);

            if (state == EMPTY || storedPsl < psl) {
                return -1; // Элемент не найден
            }

            if (state == OCCUPIED) {
//                VarHandle.acquireFence();
//                final int safetyOrderedState = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//                if (safetyOrderedState == OCCUPIED) {
                    long entryMsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long entryLsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    if (entryMsb == msb && entryLsb == lsb) {
                        final long entryCurrentOrdinalSequence = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ORDINAL_OFFSET);
                        long balance = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                        if (entryCurrentOrdinalSequence < this.committedTableOrdinal) {
                            final long stagedAmount = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                            balance += stagedAmount;
                            memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET,  balance);
                            memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ORDINAL_OFFSET,  this.committedTableOrdinal);
                        }
                        return balance;
                    }
//                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public short getAccountRate(long offset) {
        return memorySegment.get(JAVA_SHORT, offset + HASH_TABLE_ACCOUNT_RATE_OFFSET);
    }

    public long getBalance(long offset) {
        final long entryCurrentOrdinalSequence = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ORDINAL_OFFSET);
        long balance = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
        if (entryCurrentOrdinalSequence < this.committedTableOrdinal) {
            final long stagedAmount = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
            balance += stagedAmount;
            memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET,  balance);
            memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ORDINAL_OFFSET,  this.committedTableOrdinal);
        }
        return balance;
    }

    public long getOrdinal(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, maxAcceptedPsl);

        while (psl <= maxSearch) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);
            int storedPsl = memorySegment.get(JAVA_INT, offset + HASH_TABLE_PSL_OFFSET);

            if (state == EMPTY || storedPsl < psl) {
                return -1; // Элемент не найден
            }

            if (state == OCCUPIED) {
//                VarHandle.acquireFence();
//                final int safetyOrderedState = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//                if (safetyOrderedState == OCCUPIED) {
                    long entryMsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long entryLsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    if (entryMsb == msb && entryLsb == lsb) {
                        return memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    }
//                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public long getOrdinal(long offset) {
        return memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
    }

    public long getAccountTableOrdinal(long offset) {
        return memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ORDINAL_OFFSET);
    }

    public long ordinalForward(long offset) {
        final long ordinal = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
        final long nextOrdinal = nextOrdinal(ordinal);
        memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, nextOrdinal);
        return nextOrdinal;
    }

    public long getCount(UUID key) {
        if (key == null) return -1;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        int maxSearch = Math.min(maxPsl + 2, maxAcceptedPsl);

        while (psl <= maxSearch) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);
            int storedPsl = memorySegment.get(JAVA_INT, offset + HASH_TABLE_PSL_OFFSET);

            if (state == EMPTY || storedPsl < psl) {
                return -1; // Элемент не найден
            }

            if (state == OCCUPIED) {
//                VarHandle.acquireFence();
//                final int safetyOrderedState = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//                if (safetyOrderedState == OCCUPIED) {
                    long entryMsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long entryLsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    if (entryMsb == msb && entryLsb == lsb) {
                        return memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                    }
//                }
            }

            index = (index + 1) & mask;
            psl++;
        }

        return -1;
    }

    public long getCount(long offset) {
        return memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
    }

    public long countForward(long offset) {
        final long nextCount = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET) + 1;
        memorySegment.set(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, nextCount);
        return nextCount;
    }


    /**
     * Специализированные методы для отдельных полей
     */
//    public long putBalance(UUID key, long value) {
//        // Получаем существующие значения одним поиском
//        if (key == null) throw new IllegalArgumentException("Key cannot be null");
//
//        long msb = key.getMostSignificantBits();
//        long lsb = key.getLeastSignificantBits();
//        int hashCode = hash(msb, lsb);
//        int index = hashCode & mask;
//        int psl = 0;
//
//        // Пытаемся найти и обновить существующий элемент
//        while (psl <= maxPsl + 2) {
//            long offset = (long) index * MAP_ENTRY_SIZE;
//            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//
//            if (state == EMPTY) break;
//
//            if (state == OCCUPIED) {
//                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
//                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);
//
//                if (entryMsb == msb && entryLsb == lsb) {
//                    // Обновляем только balance
//                    memorySegment.set(JAVA_LONG, offset + BALANCE_OFFSET, value);
//                    return offset; // Обновление
//                }
//            }
//
//            index = (index + 1) & mask;
//            psl++;
//        }
//
//        // Элемент не найден - создаем новый с дефолтными значениями
//        return put(key, value, 0L, 0L);
//    }
//
//    public long putOrdinal(UUID key, long value) {
//        if (key == null) throw new IllegalArgumentException("Key cannot be null");
//
//        long msb = key.getMostSignificantBits();
//        long lsb = key.getLeastSignificantBits();
//        int hashCode = hash(msb, lsb);
//        int index = hashCode & mask;
//        int psl = 0;
//
//        while (psl <= maxPsl + 2) {
//            long offset = (long) index * MAP_ENTRY_SIZE;
//            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//
//            if (state == EMPTY) break;
//
//            if (state == OCCUPIED) {
//                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
//                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);
//
//                if (entryMsb == msb && entryLsb == lsb) {
//                    memorySegment.set(JAVA_LONG, offset + ORDINAL_OFFSET, value);
//                    return offset;
//                }
//            }
//
//            index = (index + 1) & mask;
//            psl++;
//        }
//
//        return put(key, 0L, value, 0L);
//    }

//    public long putCount(UUID key, long value) {
//        if (key == null) throw new IllegalArgumentException("Key cannot be null");
//
//        long msb = key.getMostSignificantBits();
//        long lsb = key.getLeastSignificantBits();
//        int hashCode = hash(msb, lsb);
//        int index = hashCode & mask;
//        int psl = 0;
//
//        while (psl <= maxPsl + 2) {
//            long offset = (long) index * MAP_ENTRY_SIZE;
//            int state = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//
//            if (state == EMPTY) break;
//
//            if (state == OCCUPIED) {
//                long entryMsb = memorySegment.get(JAVA_LONG, offset + MSB_OFFSET);
//                long entryLsb = memorySegment.get(JAVA_LONG, offset + LSB_OFFSET);
//
//                if (entryMsb == msb && entryLsb == lsb) {
//                    memorySegment.set(JAVA_LONG, offset + COUNT_OFFSET, value);
//                    return offset;
//                }
//            }
//
//            index = (index + 1) & mask;
//            psl++;
//        }
//
//        return put(key, 0L, 0L, value);
//    }

    public boolean remove(UUID key) {
        if (key == null) return false;

        long msb = key.getMostSignificantBits();
        long lsb = key.getLeastSignificantBits();
        int hashCode = hash(msb, lsb);
        int index = hashCode & mask;
        int psl = 0;

        while (psl <= maxPsl + 2) {
            long offset = (long) index * HASH_TABLE_ENTRY_SIZE;
            int state = memorySegment.get(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET);

            if (state == EMPTY) return false;

            if (state == OCCUPIED) {
                long entryMsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                long entryLsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);

                if (entryMsb == msb && entryLsb == lsb) {
                    memorySegment.set(JAVA_SHORT, offset + HASH_TABLE_STATE_OFFSET, DELETED);
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
            nextPowerOfTwo((int) (size / HASH_TABLE_LOAD_FACTOR) + HASH_TABLE_PREALLOC_CHUNK));

        resize(newCapacity);
    }

    private void resize(int newCapacity) {
        if (newCapacity <= capacity) {
            throw new IllegalArgumentException("New capacity must be greater than current");
        }

        // Сохраняем старые данные
        final MemorySegment oldMemorySegment = memorySegment;
        final int oldCapacity = capacity;
        final int oldSize = size;

        // Выделяем новую память
        this.capacity = nextPowerOfTwo(newCapacity);
        this.mask = capacity - 1;
        this.memorySegment = arena.allocate((long) HASH_TABLE_ENTRY_SIZE * capacity);
        this.size = 0;
        this.maxPsl = 0;

        initializeMemory();

        try {
            // Полный rehash с Robin Hood
            for (int oldIndex = 0; oldIndex < oldCapacity; oldIndex++) {
                long oldOffset = (long) oldIndex * HASH_TABLE_ENTRY_SIZE;
                int state = oldMemorySegment.get(JAVA_INT, oldOffset + HASH_TABLE_STATE_OFFSET);

                if (state == OCCUPIED) {
                    long msb = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long lsb = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    long balance = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                    long ordinal = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    long count = oldMemorySegment.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);

                    put(new UUID(msb, lsb), balance, ordinal, count);
                }
            }

            if (size != oldSize) {
                throw new IllegalStateException("Size mismatch after resize");
            }

        } catch (Exception e) {
            // Откат при ошибке
            this.memorySegment = oldMemorySegment;
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

        int optimalCapacity = Math.max(nextPowerOfTwo((int) (size / HASH_TABLE_LOAD_FACTOR)), HASH_TABLE_PREALLOC_CHUNK);
        resize(optimalCapacity);
    }
}