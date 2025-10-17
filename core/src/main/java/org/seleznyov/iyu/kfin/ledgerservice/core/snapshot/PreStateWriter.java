package org.seleznyov.iyu.kfin.ledgerservice.core.snapshot;

import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.seleznyov.iyu.kfin.ledgerservice.core.ringbuffer.handler.SWSRRingBufferHandler;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.zip.CRC32;

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.AccountPartitionHashTableConstants.HASH_TABLE_ENTRY_SIZE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.StateOffsetConstants.*;

public class PreStateWriter {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(PreStateWriter.class);

    private final LedgerConfiguration ledgerConfiguration;
    private final int partitionNumber;
    private final Arena arena;

    private Path indexPath;
    private Path statePath;
    private FileChannel indexFileChannel;
    private FileChannel stateFileChannel;

    private MemorySegment indexSegment;
    private MemorySegment stateSegment;

    private long nextStateOffset;

    public PreStateWriter(
        LedgerConfiguration ledgerConfiguration,
        int partitionNumber,
        Arena arena
    ) {
        this.ledgerConfiguration = ledgerConfiguration;
        this.partitionNumber = partitionNumber;
        this.arena = arena;
        final Path stateDirectory = Path.of(ledgerConfiguration.snapshot().path());
        final Path indexDirectory = Path.of(ledgerConfiguration.snapshot().path());

        this.statePath = stateDirectory;
        this.indexPath = indexDirectory;
    }

    private String indexFilename(int partitionNumber) {
        return String.format("partition-%d.state.index", partitionNumber);
    }

    private String stateFilename(int partitionNumber) {
        return String.format("partition-%d.state", partitionNumber);
    }

    private void rotateFiles() throws IOException {
        log.debug(
            "Rotating WAL file: shard={}, currentSize={}",
            this.partitionNumber,
            walFileOffset
//            walFileOffset.get()
        );

//        walSequenceId.incrementAndGet();
        walSequenceId++;
        final String filename = stateFilename(this.partitionNumber);
        closeStateFile();
        openNewStateFile(filename);
        closeCheckpointFile();
        openNewCheckpointFile(filename);
    }

    private void closeStateFile() throws IOException {
        if (stateFileChannel != null && stateFileChannel.isOpen()) {
            stateFileChannel.force(true);
            stateFileChannel.close();
        }
    }

    private void openNewStateFile(String filename) {
        try {
            final String stateFileName = filename;

            final Path filePath = statePath.resolve(stateFileName);

            // Open с Direct I/O flags
            this.stateFileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
            ); // Direct I/O

            // Pre-allocate file для better performance
            this.stateFileChannel.write(ByteBuffer.allocate(1), ledgerConfiguration.snapshot().maxStateFileSizeMb() - 1);
            this.stateFileChannel.force(true);
            this.stateFileChannel.position(0);

            this.walFileOffset = 0;

//            WAL_FILE_OFFSET_VAR_HANDLE.set(this, 0);

            log.info("Opened new WAL file: shard={}, file={}", partitionNumber, walFileName);
        } catch (IOException exception) {
            log.error("Error opening new WAL file", exception);
            throw new RuntimeException(exception);
        }
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
            StandardOpenOption.TRUNCATE_EXISTING)
        ) {

            // Вычисляем размер файла
            long indexSize = INDEX_FILE_HEADER_SIZE + ((long) indexCapacity * INDEX_SIZE);

            // Выделяем временный segment для header
            MemorySegment header = Arena.ofAuto().allocate(INDEX_FILE_HEADER_SIZE);

            // Заполняем header
            header.set(
                INDEX_MAGIC_PREFIX_TYPE,
                INDEX_MAGIC_PREFIX_OFFSET,
                INDEX_MAGIC_PREFIX
            );
            header.set(
                INDEX_FILE_HEADER_VERSION_TYPE,
                INDEX_FILE_HEADER_VERSION_OFFSET,
                1
            );
            header.set(
                INDEX_FILE_HEADER_PARTITION_NUMBER_TYPE,
                INDEX_FILE_HEADER_PARTITION_NUMBER_OFFSET,
                partitionNumber
            );
            header.set(
                INDEX_FILE_HEADER_CAPACITY_TYPE,
                INDEX_FILE_HEADER_CAPACITY_OFFSET,
                indexCapacity
            );
            header.set(
                INDEX_FILE_HEADER_COUNT_TYPE,
                INDEX_FILE_HEADER_COUNT_OFFSET,
                0
            );
            header.set(
                INDEX_FILE_HEADER_LOAD_FACTOR_TYPE,
                INDEX_FILE_HEADER_LOAD_FACTOR_OFFSET,
                (int)(LOAD_FACTOR * 100)
            );
            header.set(
                INDEX_FILE_HEADER_CURRENT_ORDINAL_TYPE,
                INDEX_FILE_HEADER_CURRENT_ORDINAL_OFFSET,
                0
            );
            header.set(
                INDEX_FILE_HEADER_COMMITTED_ORDINAL_TYPE,
                INDEX_FILE_HEADER_COMMITTED_ORDINAL_OFFSET,
                0
            );

            // Пишем header
            channel.write(header.asByteBuffer());

            // Расширяем файл до полного размера (заполнено нулями)
            channel.position(indexSize - 1);
            channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));
        }

        // ==========================================
        // DATA FILE
        // ==========================================
        try (FileChannel channel = FileChannel.open(
            statePath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)
        ) {

            MemorySegment header = Arena.ofAuto().allocate(STATE_FILE_HEADER_SIZE);

            header.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, STATE_MAGIC_PREFIX);
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
        }
    }

    /**
     * Map файлы в память
     */
    private void mapFiles() throws Exception {
        long indexSize = STATE_FILE_HEADER_SIZE + ((long) indexCapacity * INDEX_SIZE);
        this.indexSegment = indexFileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            indexSize,
            arena
        );

        long dataSize = stateFileChannel.size();
        this.stateSegment = stateFileChannel.map(
            FileChannel.MapMode.READ_WRITE,
            0,
            dataSize,
            arena
        );
    }

    public void writeState(UUID accountId, SWSRRingBufferHandler ringBufferHandler) throws Exception {
        long offset = appendToStateFile(ringBufferHandler.memorySegment(), ringBufferHandler.processOffset());

        updateIndex(ringBufferHandler.memorySegment(), accountId, offset);
    }

    /**
     * Append snapshot record в data file
     * @return offset где была записана запись
     */
    private long appendToStateFile(MemorySegment ringBufferMemorySegment, long ringBufferOffset) throws Exception {
        final long offset = nextStateOffset;

        if (offset + STATE_SIZE > stateSegment.byteSize()) {
            expandDataSegment();
        }

        writeAccountRecord(ringBufferMemorySegment, ringBufferOffset, offset);

        nextStateOffset += STATE_SIZE;

        // TODO: Обновляем header в data file (периодически, не каждый раз), для production лучше делать это в отдельном потоке или batch'ами

        return offset;
    }

    /**
     * Записать account record в data segment
     */
    private void writeAccountRecord(MemorySegment ringBufferMemorySegment, long ringBufferOffset, long offset) {
        MemorySegment.copy(
            ringBufferMemorySegment,
            ringBufferOffset,
            stateSegment,
            offset,
            HASH_TABLE_ENTRY_SIZE
        );

        stateSegment.set(
            ValueLayout.JAVA_LONG,
            offset + STATE_TIMESTAMP_OFFSET,
            LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

        stateSegment.set(
            ValueLayout.JAVA_INT,
            offset + STATE_FLAGS_OFFSET,
            0
        );

        long checksum = computeChecksum(ringBufferMemorySegment, ringBufferOffset);
        stateSegment.set(
            ValueLayout.JAVA_LONG_UNALIGNED,
            offset + STATE_CHECKSUM_OFFSET,
            checksum
        );
    }

    /**
     * Update index с новым offset для accountId (Robin Hood hashing)
     */
    private void updateIndex(UUID accountId, long fileOffset, long timestamp) {
        int hash = hash(
            accountId.getMostSignificantBits(),
            accountId.getLeastSignificantBits()
        );
        int slot = hash < 0
            ? -hash % indexCapacity
            : hash % indexCapacity;
        int psl = 0;

        // Robin Hood insertion/update
        while (psl < MAX_PSL) {
            long slotOffset = HEADER_SIZE + (slot * (long) INDEX_SIZE);

            // Читаем текущий слот
            long msb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + INDEX_ACCOUNT_MSB_OFFSET);
            long lsb = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                slotOffset + INDEX_ACCOUNT_LSB_OFFSET);

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
                    slotOffset + INDEX_FILE_OFFSET_OFFSET,
                    fileOffset);
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + INDEX_LAST_UPDATED_OFFSET,
                    timestamp);
                return;
            }

            // Robin Hood: если наш PSL больше - вытесняем
            int slotPsl = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED,
                slotOffset + INDEX_PSL_OFFSET);

            if (psl > slotPsl) {
                // Вытесняем элемент
                long slotFileOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + INDEX_FILE_OFFSET_OFFSET);
                long slotTimestamp = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    slotOffset + INDEX_LAST_UPDATED_OFFSET);

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
        indexSegment.set(ValueLayout.JAVA_LONG,
            slotOffset + INDEX_ACCOUNT_MSB_OFFSET,
            accountId.getMostSignificantBits()
        );
        indexSegment.set(ValueLayout.JAVA_LONG,
            slotOffset + INDEX_ACCOUNT_LSB_OFFSET,
            accountId.getLeastSignificantBits()
        );
        indexSegment.set(ValueLayout.JAVA_LONG,
            slotOffset + INDEX_FILE_OFFSET_OFFSET,
            fileOffset
        );
        indexSegment.set(ValueLayout.JAVA_INT,
            slotOffset + INDEX_PSL_OFFSET,
            psl
        );
        indexSegment.set(ValueLayout.JAVA_LONG,
            slotOffset + INDEX_LAST_UPDATED_OFFSET,
            timestamp
        );
    }

    /**
     * Расширить data segment когда заполнился
     */
    private void expandDataSegment() throws Exception {
        long currentSize = stateSegment.byteSize();
        long newSize = currentSize * 2;  // Удваиваем размер

        System.out.println("Expanding data file from " + currentSize +
            " to " + newSize + " bytes");

        // Закрываем текущий mapping (force перед закрытием)
        stateSegment.force();

        // Расширяем файл
        stateFileChannel.position(newSize - 1);
        stateFileChannel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));

        // Remap с новым размером
        stateSegment = stateFileChannel.map(
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
        this.stateSegment.force();

        // Обновляем nextOffset в header data file
        this.stateSegment.set(ValueLayout.JAVA_LONG, 16, nextStateOffset);

        // Еще один force для header
        this.stateSegment.force();
    }

    /**
     * Закрыть writer
     */
    public void close() throws Exception {
        // Final fsync
        fsync();

        // Закрываем channels
        this.indexFileChannel.close();
        this.stateFileChannel.close();

        System.out.println("SnapshotWriter closed for partition " + this.partitionNumber);
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

    /**
     * Вычислить CRC32 checksum
     */
    private long computeChecksum(MemorySegment segment, long baseOffset) {
        final CRC32 crc = new CRC32();
        for (int i = 0; i < HASH_TABLE_ENTRY_SIZE; i++) {
            final byte b = segment.get(ValueLayout.JAVA_BYTE, baseOffset + i);
            crc.update(b);
        }
        return crc.getValue();
    }
}
