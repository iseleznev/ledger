package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import java.lang.foreign.*;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Записывает snapshots состояний счетов в memory-mapped файлы
 *
 * Структура:
 * - Index file (.index) - Robin Hood hash table для O(1) lookup
 * - Data file (.ledger) - Append-only snapshot records
 */
public class AccountsTableFileSnapshotWriter {

    // ==========================================
    // CONSTANTS
    // ==========================================
    private static final long INDEX_MAGIC = 0x4944585231L; // "IDXR1"
    private static final long DATA_MAGIC = 0x4C4447523031L; // "LDGR01"

    private static final int HEADER_SIZE = 64;
    private static final int INDEX_ENTRY_SIZE = 36;
    private static final int DATA_RECORD_SIZE = 64;

    // Index entry offsets
    private static final int IDX_ACCOUNT_MSB_OFFSET = 0;
    private static final int IDX_ACCOUNT_LSB_OFFSET = 8;
    private static final int IDX_FILE_OFFSET_OFFSET = 16;
    private static final int IDX_PSL_OFFSET = 24;
    private static final int IDX_LAST_UPDATED_OFFSET = 28;

    // Data record offsets
    private static final int REC_ACCOUNT_MSB_OFFSET = 0;
    private static final int REC_ACCOUNT_LSB_OFFSET = 8;
    private static final int REC_BALANCE_OFFSET = 16;
    private static final int REC_ORDINAL_OFFSET = 24;
    private static final int REC_STAGED_OFFSET = 32;
    private static final int REC_TIMESTAMP_OFFSET = 40;
    private static final int REC_ACCOUNT_TYPE_OFFSET = 48;
    private static final int REC_FLAGS_OFFSET = 52;
    private static final int REC_CHECKSUM_OFFSET = 56;

    private static final int MAX_PSL = 20;
    private static final double LOAD_FACTOR = 0.75;

    // ==========================================
    // INSTANCE FIELDS
    // ==========================================
    private final int partitionId;
    private final Arena arena;

    // File channels
    private final FileChannel indexChannel;
    private final FileChannel dataChannel;

    // Memory-mapped segments
    private MemorySegment indexSegment;
    private MemorySegment dataSegment;

    // Metadata
    private final int indexCapacity;
    private long nextDataOffset;

    // Paths
    private final Path indexPath;
    private final Path dataPath;

    /**
     * Конструктор
     *
     * @param partitionId номер партиции
     * @param snapshotDir директория для snapshot файлов
     * @param expectedAccounts ожидаемое количество счетов (для sizing)
     * @param arena для управления памятью
     */
    public AccountsTableFileSnapshotWriter(
        int partitionId,
        Path snapshotDir,
        int expectedAccounts,
        Arena arena
    ) throws Exception {
        this.partitionId = partitionId;
        this.arena = arena;

        // Вычисляем capacity для hash table (с учетом load factor)
        this.indexCapacity = (int) (expectedAccounts / LOAD_FACTOR);

        // Пути к файлам
        this.indexPath = snapshotDir.resolve("partition-" + partitionId + ".index");
        this.dataPath = snapshotDir.resolve("partition-" + partitionId + ".ledger");

        // Создаем или открываем файлы
        boolean indexExists = indexPath.toFile().exists();
        boolean dataExists = dataPath.toFile().exists();

        if (!indexExists || !dataExists) {
            // Инициализируем новые файлы
            initializeFiles();
        }

        // Открываем file channels
        this.indexChannel = FileChannel.open(indexPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);

        this.dataChannel = FileChannel.open(dataPath,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE);

        // Map файлы в память
        mapFiles();

        // Читаем nextDataOffset из header
        this.nextDataOffset = dataSegment.get(
            ValueLayout.JAVA_LONG_UNALIGNED,
            16  // offset в header
        );

        System.out.println("SnapshotWriter initialized for partition " + partitionId +
            ", index capacity: " + indexCapacity +
            ", next data offset: " + nextDataOffset);
    }

    /**
     * Инициализация новых файлов с headers
     */
    private void initializeFiles() throws Exception {
        // ==========================================
        // INDEX FILE
        // ==========================================
        try (FileChannel channel = FileChannel.open(indexPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            // Вычисляем размер файла
            long indexSize = HEADER_SIZE + ((long) indexCapacity * INDEX_ENTRY_SIZE);

            // Выделяем временный segment для header
            MemorySegment header = Arena.ofAuto().allocate(HEADER_SIZE);

            // Заполняем header
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, INDEX_MAGIC);
            header.set(ValueLayout.JAVA_INT_UNALIGNED, 8, 1);  // version
            header.set(ValueLayout.JAVA_INT_UNALIGNED, 12, partitionId);
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 16, (long) indexCapacity);
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 24, 0L);  // count
            header.set(ValueLayout.JAVA_INT_UNALIGNED, 32, (int)(LOAD_FACTOR * 100));
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 36, System.currentTimeMillis());
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 44, System.currentTimeMillis());

            // Пишем header
            channel.write(header.asByteBuffer());

            // Расширяем файл до полного размера (заполнено нулями)
            channel.position(indexSize - 1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));

            System.out.println("Created index file: " + indexPath +
                ", size: " + indexSize + " bytes");
        }

        // ==========================================
        // DATA FILE
        // ==========================================
        try (FileChannel channel = FileChannel.open(dataPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {

            MemorySegment header = Arena.ofAuto().allocate(HEADER_SIZE);

            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, DATA_MAGIC);
            header.set(ValueLayout.JAVA_INT_UNALIGNED, 8, 1);  // version
            header.set(ValueLayout.JAVA_INT_UNALIGNED, 12, partitionId);
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 16, (long) HEADER_SIZE);  // nextOffset
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 24, 0L);  // recordCount
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 32, System.currentTimeMillis());
            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 40, 0L);  // lastCompact

            channel.write(header.asByteBuffer());

            // Начальный размер data file (например, 10MB)
            long initialSize = 10 * 1024 * 1024;
            channel.position(initialSize - 1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));

            System.out.println("Created data file: " + dataPath +
                ", size: " + initialSize + " bytes");
        }
    }

    /**
     * Map файлы в память
     */
    private void mapFiles() throws Exception {
        // Index file - READ_WRITE
        long indexSize = HEADER_SIZE + ((long) indexCapacity * INDEX_ENTRY_SIZE);
        this.indexSegment = indexChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            indexSize,
            arena
        );

        // Data file - READ_WRITE
        long dataSize = dataChannel.size();
        this.dataSegment = dataChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            dataSize,
            arena
        );

        System.out.println("Mapped files: index=" + indexSize +
            " bytes, data=" + dataSize + " bytes");
    }

    /**
     * ГЛАВНЫЙ МЕТОД: Записать snapshot счета
     */
    public void writeSnapshot(UUID accountId, AccountSnapshot snapshot) throws Exception {
        // 1. Append в data file
        long offset = appendToDataFile(snapshot);

        // 2. Update index
        updateIndex(accountId, offset, snapshot.timestamp());
    }

    /**
     * Append snapshot record в data file
     * @return offset где была записана запись
     */
    private long appendToDataFile(AccountSnapshot snapshot) throws Exception {
        long offset = nextDataOffset;

        // Проверяем нужно ли расширить data segment
        if (offset + DATA_RECORD_SIZE > dataSegment.byteSize()) {
            expandDataSegment();
        }

        // Пишем в data segment
        writeAccountRecord(offset, snapshot);

        // Обновляем nextDataOffset
        nextDataOffset += DATA_RECORD_SIZE;

        // Обновляем header в data file (периодически, не каждый раз)
        // Для production лучше делать это в отдельном потоке или batch'ами

        return offset;
    }

    /**
     * Записать account record в data segment
     */
    private void writeAccountRecord(long offset, AccountSnapshot snapshot) {
        // Account ID (16 bytes)
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ACCOUNT_MSB_OFFSET,
            snapshot.accountId().getMostSignificantBits());
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ACCOUNT_LSB_OFFSET,
            snapshot.accountId().getLeastSignificantBits());

        // Balance (8 bytes)
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_BALANCE_OFFSET,
            snapshot.balance());

        // Ordinal (8 bytes)
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ORDINAL_OFFSET,
            snapshot.ordinal());

        // Staged amount (8 bytes) - всегда 0 для committed snapshot
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_STAGED_OFFSET,
            0L);

        // Timestamp (8 bytes)
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_TIMESTAMP_OFFSET,
            snapshot.timestamp());

        // Account type (4 bytes)
        dataSegment.set(ValueLayout.JAVA_INT_UNALIGNED,
            offset + REC_ACCOUNT_TYPE_OFFSET,
            snapshot.accountType());

        // Flags (4 bytes) - reserved
        dataSegment.set(ValueLayout.JAVA_INT_UNALIGNED,
            offset + REC_FLAGS_OFFSET,
            0);

        // Checksum (8 bytes) - CRC32 первых 56 bytes
        long checksum = computeChecksum(dataSegment, offset, REC_CHECKSUM_OFFSET);
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_CHECKSUM_OFFSET,
            checksum);
    }

    /**
     * Update index с новым offset для accountId (Robin Hood hashing)
     */
    private void updateIndex(UUID accountId, long fileOffset, long timestamp) {
        int hash = hash(accountId);
        int slot = Math.abs(hash % indexCapacity);
        int psl = 0;

        // Robin Hood insertion/update
        while (psl < MAX_PSL) {
            long slotOffset = HEADER_SIZE + (slot * (long) INDEX_ENTRY_SIZE);

            // Читаем текущий слот
            long msb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + IDX_ACCOUNT_MSB_OFFSET);
            long lsb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + IDX_ACCOUNT_LSB_OFFSET);

            // Empty slot - вставляем
            if (msb == 0 && lsb == 0) {
                writeIndexEntry(slotOffset, accountId, fileOffset, psl, timestamp);
                return;
            }

            // Нашли существующую запись - обновляем
            UUID slotAccountId = new UUID(msb, lsb);
            if (slotAccountId.equals(accountId)) {
                // IN-PLACE UPDATE - меняем только offset и timestamp
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + IDX_FILE_OFFSET_OFFSET,
                    fileOffset);
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + IDX_LAST_UPDATED_OFFSET,
                    timestamp);
                return;
            }

            // Robin Hood: если наш PSL больше - вытесняем
            int slotPsl = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED,
                slotOffset + IDX_PSL_OFFSET);

            if (psl > slotPsl) {
                // Вытесняем элемент
                long slotFileOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + IDX_FILE_OFFSET_OFFSET);
                long slotTimestamp = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + IDX_LAST_UPDATED_OFFSET);

                // Пишем наш элемент
                writeIndexEntry(slotOffset, accountId, fileOffset, psl, timestamp);

                // Продолжаем вставлять вытесненный
                accountId = slotAccountId;
                fileOffset = slotFileOffset;
                timestamp = slotTimestamp;
                psl = slotPsl;
            }

            slot = (slot + 1) % indexCapacity;
            psl++;
        }

        throw new IllegalStateException(
            "Index full or PSL exceeded MAX_PSL for account " + accountId);
    }

    /**
     * Записать entry в index slot
     */
    private void writeIndexEntry(
        long slotOffset,
        UUID accountId,
        long fileOffset,
        int psl,
        long timestamp
    ) {
        indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            slotOffset + IDX_ACCOUNT_MSB_OFFSET,
            accountId.getMostSignificantBits());
        indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            slotOffset + IDX_ACCOUNT_LSB_OFFSET,
            accountId.getLeastSignificantBits());
        indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            slotOffset + IDX_FILE_OFFSET_OFFSET,
            fileOffset);
        indexSegment.set(ValueLayout.JAVA_INT_UNALIGNED,
            slotOffset + IDX_PSL_OFFSET,
            psl);
        indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
            slotOffset + IDX_LAST_UPDATED_OFFSET,
            timestamp);
    }

    /**
     * Расширить data segment когда заполнился
     */
    private void expandDataSegment() throws Exception {
        long currentSize = dataSegment.byteSize();
        long newSize = currentSize * 2;  // Удваиваем размер

        System.out.println("Expanding data file from " + currentSize +
            " to " + newSize + " bytes");

        // Закрываем текущий mapping (force перед закрытием)
        dataSegment.force();

        // Расширяем файл
        dataChannel.position(newSize - 1);
        dataChannel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));

        // Remap с новым размером
        dataSegment = dataChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            newSize,
            arena
        );

        System.out.println("Data file expanded successfully");
    }

    /**
     * FSYNC - синхронизация с диском
     * Это КРИТИЧНЫЙ метод для durability!
     */
    public void fsync() throws Exception {
        // Force index segment (memory → disk)
        indexSegment.force();

        // Force data segment (memory → disk)
        dataSegment.force();

        // Обновляем nextOffset в header data file
        dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 16, nextDataOffset);

        // Еще один force для header
        dataSegment.force();
    }

    /**
     * Загрузить текущий snapshot счета (для чтения при обновлении)
     */
    public AccountSnapshot loadCurrentSnapshot(UUID accountId) {
        // Lookup в index
        long fileOffset = indexLookup(accountId);

        if (fileOffset == -1) {
            // Счет не найден - создаем начальный snapshot
            return new AccountSnapshot(
                accountId,
                0L,  // начальный balance
                0L,  // начальный ordinal
                0,   // account type (cold по умолчанию)
                System.currentTimeMillis()
            );
        }

        // Читаем из data file
        return readAccountRecord(fileOffset, accountId);
    }

    /**
     * Robin Hood lookup в index
     */
    private long indexLookup(UUID accountId) {
        int hash = hash(accountId);
        int slot = Math.abs(hash % indexCapacity);
        int psl = 0;

        while (psl < MAX_PSL) {
            long slotOffset = HEADER_SIZE + (slot * (long) INDEX_ENTRY_SIZE);

            long msb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + IDX_ACCOUNT_MSB_OFFSET);
            long lsb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + IDX_ACCOUNT_LSB_OFFSET);

            // Empty slot
            if (msb == 0 && lsb == 0) {
                return -1;
            }

            UUID slotAccountId = new UUID(msb, lsb);

            // Found!
            if (slotAccountId.equals(accountId)) {
                return indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + IDX_FILE_OFFSET_OFFSET);
            }

            // Robin Hood optimization
            int slotPsl = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED,
                slotOffset + IDX_PSL_OFFSET);

            if (psl > slotPsl) {
                return -1;  // Would have been found earlier
            }

            slot = (slot + 1) % indexCapacity;
            psl++;
        }

        return -1;
    }

    /**
     * Прочитать account record из data file
     */
    private AccountSnapshot readAccountRecord(long offset, UUID expectedAccountId) {
        // Account ID
        long msb = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ACCOUNT_MSB_OFFSET);
        long lsb = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ACCOUNT_LSB_OFFSET);
        UUID accountId = new UUID(msb, lsb);

        // Paranoid check
        if (!accountId.equals(expectedAccountId)) {
            throw new IllegalStateException("Corrupted data: expected " +
                expectedAccountId + " but found " + accountId);
        }

        // Balance
        long balance = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_BALANCE_OFFSET);

        // Ordinal
        long ordinal = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_ORDINAL_OFFSET);

        // Timestamp
        long timestamp = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_TIMESTAMP_OFFSET);

        // Account type
        int accountType = dataSegment.get(ValueLayout.JAVA_INT_UNALIGNED,
            offset + REC_ACCOUNT_TYPE_OFFSET);

        // Verify checksum
        long storedChecksum = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
            offset + REC_CHECKSUM_OFFSET);
        long computedChecksum = computeChecksum(dataSegment, offset, REC_CHECKSUM_OFFSET);

        if (storedChecksum != computedChecksum) {
            throw new IllegalStateException("Checksum mismatch for account " + accountId);
        }

        return new AccountSnapshot(
            accountId,
            balance,
            ordinal,
            accountType,
            timestamp
        );
    }

    /**
     * Hash function для UUID
     */
    private int hash(UUID uuid) {
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        long h = msb ^ lsb;
        h ^= (h >>> 32);
        return (int) h;
    }

    /**
     * Вычислить CRC32 checksum
     */
    private long computeChecksum(MemorySegment segment, long baseOffset, int length) {
        CRC32 crc = new CRC32();
        for (int i = 0; i < length; i++) {
            byte b = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i);
            crc.update(b);
        }
        return crc.getValue();
    }

    /**
     * Закрыть writer
     */
    public void close() throws Exception {
        // Final fsync
        fsync();

        // Закрываем channels
        indexChannel.close();
        dataChannel.close();

        System.out.println("SnapshotWriter closed for partition " + partitionId);
    }
}