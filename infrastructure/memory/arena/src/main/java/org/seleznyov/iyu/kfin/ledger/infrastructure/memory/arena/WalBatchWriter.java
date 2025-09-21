package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import com.sun.nio.file.ExtendedOpenOption;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.BatchRingBufferHandler;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.EntryRecordBatchHandler;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class WalBatchWriter {

    private final static int CPU_CACHE_LINE_SIZE = 64;

    //LEDGERPX
    private static final long WAL_MAGIC = ByteBuffer.wrap(
        new byte[]{76, 69, 68, 71, 69, 82, 80, 88}
    ).getLong();

    private static final long WAL_MAGIC_PREFIX_OFFSET = 0;
    private static final ValueLayout WAL_MAGIC_TYPE = ValueLayout.JAVA_LONG;

    private static final long WAL_SEQUENCE_ID_OFFSET = WAL_MAGIC_PREFIX_OFFSET + WAL_MAGIC_TYPE.byteSize();
    private static final ValueLayout WAL_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final long ENTRY_RECORD_SIZE_OFFSET = WAL_SEQUENCE_ID_OFFSET + WAL_SEQUENCE_ID_TYPE.byteSize();
    private static final ValueLayout ENTRY_RECORD_SIZE_TYPE = ValueLayout.JAVA_LONG;

    private static final long WAL_BATCH_ENTRIES_COUNT_OFFSET = ENTRY_RECORD_SIZE_OFFSET + ENTRY_RECORD_SIZE_TYPE.byteSize();
    private static final ValueLayout WAL_BATCH_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final long HEADER_RAW_SIZE = WAL_BATCH_ENTRIES_COUNT_OFFSET + WAL_BATCH_ENTRIES_COUNT_TYPE.byteSize();
    private static final long HEADER_SIZE = (HEADER_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    private static final byte[] HEADER_PADDING = new byte[64 - (int) HEADER_SIZE];

    private static final long CHECKPOINT_SEQUENCE_ID_OFFSET = 0;
    private static final ValueLayout CHECKPOINT_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final long CHECKPOINT_WAL_FILE_SIZE_OFFSET = CHECKPOINT_SEQUENCE_ID_OFFSET + CHECKPOINT_SEQUENCE_ID_TYPE.byteSize();
    private static final ValueLayout CHECKPOINT_WAL_FILE_SIZE_TYPE = ValueLayout.JAVA_LONG;

    private static final long CHECKPOINT_RAW_SIZE = CHECKPOINT_WAL_FILE_SIZE_OFFSET + CHECKPOINT_WAL_FILE_SIZE_TYPE.byteSize();
    private static final long CHECKPOINT_SIZE = (CHECKPOINT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    private static final byte[] CHECKPOINT_PADDING = new byte[64 - (int) CHECKPOINT_SIZE];

    private final Path walDirectory;
    private final int shardId;
    private final AtomicLong walFileSequence = new AtomicLong(1);
    private final AtomicLong walFileOffset = new AtomicLong(0);
    private final ByteBuffer writeBuffer;
    private FileChannel walFileChannel;
    private FileChannel checkpointFileChannel;
    private volatile boolean isOpen = false;
    private final WalConfiguration configuration;
    private final long maxFileSize;

//    private long sequenceId = 0;

    public WalBatchWriter(
        WalConfiguration configuration,
        int shardId,
        long walFileOffset
    ) {
        this.shardId = shardId;


        // Open first file
        final Path walDirectory = Path.of(configuration.path());

        this.configuration = configuration;
        this.walDirectory = walDirectory;
        this.writeBuffer = ByteBuffer.allocateDirect(
            alignSize(
                configuration.writeFileBufferSize(),
                configuration.alignmentSize()
            )
        );
        this.maxFileSize = configuration.maxFileSizeMb() * 1024 * 1024;
//        this.sequenceId = sequenceId;
        this.walFileOffset.set(walFileOffset);
    }

    public void initialize() {
        createDirectory(this.walDirectory);
        final String filename = newFilename();
        openNewWalFile(filename);
        openNewCheckpointFile(filename);
    }

    private void createDirectory(Path walDirectory) {
        try {
            Files.createDirectories(walDirectory);
        } catch (IOException exception) {
            log.error("Error creating directory", exception);
            throw new RuntimeException(exception);
        }
    }

    private String newFilename() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return String.format(
            "%s%02d_%06d_%s",
            configuration.walFilePrefix(), shardId,
            walFileSequence.get(),
            timestamp
        );
    }

    private String newWalFilename(String newFilename) {
        return newFilename + ".wal";
    }

    private String newCheckpointFilename(String newFilename) {
        return newFilename + "_checkpoint.dat";
    }

    private void closeFileChannel() {
        try {
            if (walFileChannel != null && walFileChannel.isOpen()) {
                walFileChannel.force(true);
                walFileChannel.close();
            }
        } catch (IOException exception) {
            log.error("Error closing file channel", exception);
            throw new RuntimeException(exception);
        }
    }

    public long writeBatch(
        BatchRingBufferHandler ringBufferHandler,
        long walSequenceId,
        long batchSlotOffset,
        long batchRawSize
    ) {
        try {
            final long fileOffset = walFileOffset.getAndAdd(batchRawSize + HEADER_SIZE);

            if (walFileOffset.get() >= maxFileSize) {
                rotateFiles();
            }

            writeBuffer.clear();

            if (batchRawSize - HEADER_SIZE <= maxFileSize) {
                final MemorySegment memorySegment = ringBufferHandler.ringBufferSegment().asSlice(batchSlotOffset, batchRawSize);
                writeBuffer.putLong(WAL_MAGIC);
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.putLong(batchRawSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.put(HEADER_PADDING);
                writeBuffer.flip();

                writeBuffer.put(
                    memorySegment.asByteBuffer()
                );
                long bytesWritten = 0;
                while (writeBuffer.hasRemaining()) {
                    bytesWritten += walFileChannel.write(writeBuffer);
                }

                writeBuffer.clear();
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(fileOffset);
                writeBuffer.put(CHECKPOINT_PADDING);
                writeBuffer.flip();

                while (writeBuffer.hasRemaining()) {
                    bytesWritten += checkpointFileChannel.write(writeBuffer);
                }

                return bytesWritten;

            } else {
                // Multiple writes для больших batches
                long totalWritten = 0;
                long offset = batchSlotOffset;

                writeBuffer.clear();
                writeBuffer.putLong(WAL_MAGIC);
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.putLong(batchRawSize / EntryRecordBatchHandler.POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.put(HEADER_PADDING);

                while (offset < batchSlotOffset + batchRawSize) {
                    int chunkSize = Math.min(
                        configuration.writeFileBufferSize(),
                        (int) batchRawSize
                    );
                    final MemorySegment memorySegment = ringBufferHandler.ringBufferSegment().asSlice(offset, chunkSize);
                    writeBuffer.put(
                        memorySegment.asByteBuffer()
                    );
                    writeBuffer.flip();

                    while (writeBuffer.hasRemaining()) {
                        totalWritten += walFileChannel.write(writeBuffer);
                    }

                    writeBuffer.clear();
                    offset += chunkSize;
                }

                writeBuffer.clear();
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(fileOffset);
                writeBuffer.put(CHECKPOINT_PADDING);
                writeBuffer.flip();

                return totalWritten;
            }
        } catch (IOException exception) {
            log.error("Error writing batch to file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void openNewWalFile(String filename) {
        try {
            final String walFileName = newWalFilename(filename);

            final Path filePath = walDirectory.resolve(walFileName);

            // Open с Direct I/O flags
            this.walFileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                ExtendedOpenOption.DIRECT
            ); // Direct I/O

            // Pre-allocate file для better performance
            this.walFileChannel.write(ByteBuffer.allocate(1), maxFileSize - 1);
            this.walFileChannel.force(true);
            this.walFileChannel.position(0);

            walFileOffset.set(0);

            log.info("Opened new WAL file: shard={}, file={}", shardId, walFileName);
        } catch (IOException exception) {
            log.error("Error opening new WAL file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void openNewCheckpointFile(String walFilename) {
        try {
            final String checkpointFileName = newWalFilename(newCheckpointFilename(walFilename));


            final Path filePath = walDirectory.resolve(checkpointFileName);

            // Open с Direct I/O flags
            this.checkpointFileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.APPEND,
                ExtendedOpenOption.DIRECT
            ); // Direct I/O

            // Pre-allocate file для better performance
//            this.checkpointFileChannel.write(ByteBuffer.allocate(1), maxFileSize - 1);
            this.checkpointFileChannel.force(true);
            this.checkpointFileChannel.position(0);

            walFileOffset.set(0);

            log.info("Opened new checkpoint file: shard={}, file={}", shardId, checkpointFileName);
        } catch (IOException exception) {
            log.error("Error opening new checkpoint file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void rotateFiles() throws IOException {
        log.debug(
            "Rotating WAL file: shard={}, currentSize={}",
            shardId,
            walFileOffset.get()
        );

        walFileSequence.incrementAndGet();
        final String filename = newFilename();
        closeWalFile();
        openNewWalFile(filename);
        closeCheckpointFile();
        openNewCheckpointFile(filename);
    }

    private void closeWalFile() throws IOException {
        if (walFileChannel != null && walFileChannel.isOpen()) {
            walFileChannel.force(true);
            walFileChannel.close();
        }
    }

    private void closeCheckpointFile() throws IOException {
        if (walFileChannel != null && walFileChannel.isOpen()) {
            walFileChannel.force(true);
            walFileChannel.close();
        }
    }

    private int alignSize(int size, int alignment) {
        return ((size + alignment - 1) / alignment) * alignment;
    }

    private long calculateChecksum(ByteBuffer buffer, int offset, int length) {
        long checksum = 0;
        for (int i = offset; i < offset + length - 8; i += 8) {
            if (i + 8 <= buffer.capacity()) {
                checksum ^= buffer.getLong(i);
            }
        }
        return checksum;
    }
}
