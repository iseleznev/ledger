package org.seleznyov.iyu.kfin.ledgerservice.core.hashtable;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.UUID;

import static java.lang.foreign.ValueLayout.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.AccountPartitionHashTableConstants.*;
import static org.seleznyov.iyu.kfin.ledgerservice.core.utils.OrdinalUtils.nextOrdinal;

public class AccountPartitionRegularResizeHashTable implements AccountPartitionHashTable {

    private final static int MAX_EXPANSION_ATTEMPTS = 3;

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

    public AccountPartitionRegularResizeHashTable(Arena arena, int expectedCapacity, int maxAcceptedPsl) {
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
     * Robin Hood put - основной метод вставки
     */
    public long put(UUID accountId, long balance, long ordinal, long count) {
        if (accountId == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        int expansionAttempts = 0;

        while (true) {
            if (size >= capacity * HASH_TABLE_LOAD_FACTOR) {
                if (expansionAttempts >= MAX_EXPANSION_ATTEMPTS) {
                    throw new IllegalStateException(
                        "Hash table expansion failed after " + MAX_EXPANSION_ATTEMPTS +
                            " attempts. Capacity: " + capacity + ", Size: " + size
                    );
                }

                long oldCapacity = capacity;
                expandCapacity();
                expansionAttempts++;

                // Verify expansion actually happened
                if (capacity <= oldCapacity) {
                    throw new IllegalStateException(
                        "expandCapacity() failed to increase capacity from " + oldCapacity
                    );
                }
            }

            if (size >= capacity * HASH_TABLE_LOAD_FACTOR) {
                expandCapacity();
            }

            long accountIdMsb = accountId.getMostSignificantBits();
            long accountIdLsb = accountId.getLeastSignificantBits();
            int hashCode = hash(accountIdMsb, accountIdLsb);
            int idealIndex = hashCode & mask;

            // Элемент для вставки
            long insertMsb = accountIdMsb;
            long insertLsb = accountIdLsb;
            long insertBalance = balance;
            long insertOrdinal = ordinal;
            long insertStagedAmount = 0;
            long insertCurrentOrdinalSequence = this.committedTableOrdinal;
            long insertCount = count;
            short insertAccountRate = RATE_COLD_ACCOUNT;

            int currentIndex = idealIndex;
            int currentPsl = 0;

            while (currentPsl <= maxAcceptedPsl) {
                long currentOffset = (long) currentIndex * HASH_TABLE_ENTRY_SIZE;
                int state = memorySegment.get(JAVA_SHORT, currentOffset + HASH_TABLE_STATE_OFFSET);

                if (state == EMPTY || state == DELETED) {
                    // Вставляем элемент - inline операции для скорости
                    memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_STATE_OFFSET, OCCUPIED);
//                    VarHandle.releaseFence();
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET, insertMsb);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET, insertLsb);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                    memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertCurrentOrdinalSequence);
                    memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);
                    memorySegment.set(JAVA_INT, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);
//                    VarHandle.releaseFence();

                    if (state == EMPTY) size++;
                    if (currentPsl > maxPsl) maxPsl = currentPsl;
                    totalProbes += currentPsl + 1;
                    return currentOffset;
                }

                if (state == OCCUPIED) {
                    long existingMsb = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long existingLsb = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);

                    // Проверяем на обновление существующего ключа
                    if (existingMsb == insertMsb && existingLsb == insertLsb) {
                        // Обновление - inline операции
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertCurrentOrdinalSequence);
                        memorySegment.set(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);
                        memorySegment.set(JAVA_INT, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);
//                        VarHandle.releaseFence();
                        return currentOffset; // Обновление, не вставка
                    }

                    // Robin Hood логика: вычисляем PSL существующего элемента
                    int existingHash = hash(existingMsb, existingLsb);
                    int existingIdeal = existingHash & mask;
                    int existingPsl = (currentIndex - existingIdeal + capacity) & mask;

                    // Если наш PSL больше - вытесняем существующий элемент
                    if (currentPsl > existingPsl) {
                        // Сохраняем существующий элемент для дальнейшей вставки
//                        MemorySegment.copy(memorySegment, currentOffset + MSB_OFFSET, memorySegment, currentOffset + SHADOW_MSB_OFFSET, VALUE_ACTIVE_AREA_SIZE);
//                        VarHandle.releaseFence();
//                        memorySegment.set(JAVA_INT, currentOffset + STATE_OFFSET, REPLACING);
                        long tempBalance = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                        long tempOrdinal = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                        long tempCount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);
                        long tempCurrentOrdinalSequence = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET);
                        long tempStagedAmount = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET);
                        short tempAccountRate = memorySegment.get(JAVA_SHORT, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET);

                        // Записываем наш элемент на место существующего
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET, insertMsb);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET, insertLsb);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, insertBalance);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, insertOrdinal);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, insertCount);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, insertCurrentOrdinalSequence);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, insertStagedAmount);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_PSL_OFFSET, currentPsl);
                        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_RATE_OFFSET, insertAccountRate);
//                        VarHandle.releaseFence();

                        // Продолжаем с вытесненным элементом
                        insertMsb = existingMsb;
                        insertLsb = existingLsb;
                        insertBalance = tempBalance;
                        insertOrdinal = tempOrdinal;
                        insertCount = tempCount;
                        insertStagedAmount = tempStagedAmount;
                        insertCurrentOrdinalSequence = tempCurrentOrdinalSequence;
                        insertAccountRate = tempAccountRate;
                        currentPsl = existingPsl;

                    }
                }

                currentIndex = (currentIndex + 1) & mask;
                currentPsl++;
            }

            // Если PSL превысил лимит - расширяем таблицу и пытаемся снова
            if (expansionAttempts >= MAX_EXPANSION_ATTEMPTS) {
                throw new IllegalStateException(
                    "Unable to insert element after " + MAX_EXPANSION_ATTEMPTS +
                        " capacity expansions. Key: " + accountId + ", PSL limit: " + maxAcceptedPsl);
            }

            long oldCapacity = capacity;
            expandCapacity();
            expansionAttempts++;

            if (capacity <= oldCapacity) {
                throw new IllegalStateException(
                    "expandCapacity() failed to increase capacity from " + oldCapacity
                );
            }
        }
    }

    public long put(UUID accountId) {
        return put(accountId, 0, 0, 0);
    }

    public void update(long currentOffset, long balance, long ordinal, long count) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, balance);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, ordinal);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, count);
//        memorySegment.set(JAVA_LONG, currentOffset + CURRENT_ORDINAL_SEQUENCE_OFFSET, this.currentOrdinalSequence);
    }

    public void updateAccountTableCurrentOrdinal(long currentOffset, long currentOrdinalSequence) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ORDINAL_OFFSET, currentOrdinalSequence);
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

    public void updateBalance(long currentOffset, long balance) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET, balance);
    }

    public void increaseBalance(long currentOffset, long increaseAmount) {
        final long balance = memorySegment.get(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_STAGED_AMOUNT_OFFSET, balance + increaseAmount);
    }

    public void updateOrdinal(long currentOffset, long ordinal) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET, ordinal);
    }

    public void updateCount(long currentOffset, long count) {
        memorySegment.set(JAVA_LONG, currentOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET, count);
    }

    public long getOffset(UUID key) {
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

            if (state == EMPTY) return -1;

            if (state == OCCUPIED) {
//                VarHandle.acquireFence();
//                final int safetyOrderedState = memorySegment.get(JAVA_INT, offset + STATE_OFFSET);
//                if (safetyOrderedState == OCCUPIED) {
                    long entryMsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long entryLsb = memorySegment.get(JAVA_LONG, offset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    if (entryMsb == msb && entryLsb == lsb) {
                        return offset;
                    }
//                }
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
        MemorySegment oldMemory = memorySegment;
        int oldCapacity = capacity;
        int oldSize = size;

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
                int state = oldMemory.get(JAVA_INT, oldOffset + HASH_TABLE_STATE_OFFSET);

                if (state == OCCUPIED) {
                    long msb = oldMemory.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_MSB_OFFSET);
                    long lsb = oldMemory.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ID_LSB_OFFSET);
                    long balance = oldMemory.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_BALANCE_OFFSET);
                    long ordinal = oldMemory.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ORDINAL_OFFSET);
                    long count = oldMemory.get(JAVA_LONG, oldOffset + HASH_TABLE_ACCOUNT_ENTRIES_COUNT_OFFSET);

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

        int optimalCapacity = Math.max(nextPowerOfTwo((int) (size / HASH_TABLE_LOAD_FACTOR)), HASH_TABLE_PREALLOC_CHUNK);
        resize(optimalCapacity);
    }
}