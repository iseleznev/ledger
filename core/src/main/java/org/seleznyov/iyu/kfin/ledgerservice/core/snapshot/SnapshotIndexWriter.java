package org.seleznyov.iyu.kfin.ledgerservice.core.snapshot;

import com.sun.nio.file.ExtendedOpenOption;
import org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties.LedgerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.CommonConstants.CPU_CACHE_LINE_SIZE;
import static org.seleznyov.iyu.kfin.ledgerservice.core.constants.EntryRecordOffsetConstants.POSTGRES_ENTRY_RECORD_SIZE;

public class SnapshotIndexWriter {

    private static final Logger log = LoggerFactory.getLogger(SnapshotIndexWriter.class);

//    private static final VarHandle WAL_SEQUENCE_ID_VAR_HANDLE;
//    private static final VarHandle WAL_FILE_OFFSET_VAR_HANDLE;
//
//    static {
//        try {
//            MethodHandles.Lookup lookup = MethodHandles.lookup();
//            WAL_SEQUENCE_ID_VAR_HANDLE = lookup.findVarHandle(
//                WalWriter.class, "walSequenceId", long.class);
//            WAL_FILE_OFFSET_VAR_HANDLE = lookup.findVarHandle(
//                WalWriter.class, "walFileOffset", long.class);
//        } catch (Exception e) {
//            throw new ExceptionInInitializerError(e);
//        }
//    }

    private static final long WAL_MAGIC_PREFIX = 0x4C4447525368574CL; // "LDGRSNWL"

    private static final long WAL_MAGIC_PREFIX_OFFSET = 0;
    private static final ValueLayout.OfLong WAL_MAGIC_TYPE = ValueLayout.JAVA_LONG;

    private static final long WAL_SEQUENCE_ID_OFFSET = WAL_MAGIC_PREFIX_OFFSET + WAL_MAGIC_TYPE.byteSize();
    private static final ValueLayout.OfLong WAL_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final long ENTRY_RECORD_SIZE_OFFSET = WAL_SEQUENCE_ID_OFFSET + WAL_SEQUENCE_ID_TYPE.byteSize();
    private static final ValueLayout.OfLong ENTRY_RECORD_SIZE_TYPE = ValueLayout.JAVA_LONG;

    private static final long WAL_BATCH_ENTRIES_COUNT_OFFSET = ENTRY_RECORD_SIZE_OFFSET + ENTRY_RECORD_SIZE_TYPE.byteSize();
    private static final ValueLayout.OfLong WAL_BATCH_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final long HEADER_RAW_SIZE = WAL_BATCH_ENTRIES_COUNT_OFFSET + WAL_BATCH_ENTRIES_COUNT_TYPE.byteSize();
    private static final long HEADER_SIZE = (HEADER_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    private static final byte[] HEADER_PADDING = new byte[64 - (int) HEADER_SIZE];

    private static final long CHECKPOINT_SEQUENCE_ID_OFFSET = 0;
    private static final ValueLayout.OfLong CHECKPOINT_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final long CHECKPOINT_WAL_FILE_SIZE_OFFSET = CHECKPOINT_SEQUENCE_ID_OFFSET + CHECKPOINT_SEQUENCE_ID_TYPE.byteSize();
    private static final ValueLayout.OfLong CHECKPOINT_WAL_FILE_SIZE_TYPE = ValueLayout.JAVA_LONG;

    private static final long CHECKPOINT_RAW_SIZE = CHECKPOINT_WAL_FILE_SIZE_OFFSET + CHECKPOINT_WAL_FILE_SIZE_TYPE.byteSize();
    private static final long CHECKPOINT_SIZE = (CHECKPOINT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    private static final byte[] CHECKPOINT_PADDING = new byte[64 - (int) CHECKPOINT_SIZE];

    private final Path walDirectory;
    private final int partitionNumber;
    private long walSequenceId = 0;
    private long walFileOffset = 0;
    private final ByteBuffer writeBuffer;
    private FileChannel walFileChannel;
    private FileChannel checkpointFileChannel;
    private volatile boolean isOpen = false;
    private final LedgerConfiguration ledgerConfiguration;
    private final long maxFileSize;

//    private long sequenceId = 0;

    public SnapshotIndexWriter(
        LedgerConfiguration ledgerConfiguration,
        int partitionNumber,
        long walFileOffset
    ) {
        this.partitionNumber = partitionNumber;

        // Open first file
        final Path walDirectory = Path.of(ledgerConfiguration.wal().path());

        this.ledgerConfiguration = ledgerConfiguration;
        this.walDirectory = walDirectory;
        this.writeBuffer = ByteBuffer.allocateDirect(
            alignSize(
                ledgerConfiguration.wal().writeFileBufferSize(),
                ledgerConfiguration.wal().alignmentSize()
            )
        );
        this.maxFileSize = ledgerConfiguration.wal().maxFileSizeMb() * 1024 * 1024;
        this.walFileOffset = walFileOffset;
//        WAL_FILE_OFFSET_VAR_HANDLE.set(this, walFileOffset);
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
            ledgerConfiguration.wal().walFilePrefix(), partitionNumber,
            walSequenceId,
//            (long) WAL_SEQUENCE_ID_VAR_HANDLE.get(this),
            timestamp
        );
    }

    private String newWalFilename(String newFilename) {
        return newFilename + "_snapshot.idx";
    }

    private String newCheckpointFilename(String newFilename) {
        return newFilename + "_snapshot_checkpoint.idx";
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

    public long write(
        MemorySegment ringBufferMemorySegment,
        long walSequenceId,
        long batchOffset,
        long batchSize
    ) {
        try {
            final long fileOffset = walFileOffset;//(long) WAL_FILE_OFFSET_VAR_HANDLE.get(this);
            if (fileOffset >= maxFileSize) {
                rotateFiles();
            }

            writeBuffer.clear();

            if (fileOffset + batchSize + HEADER_SIZE <= maxFileSize) {
                writeBuffer.putLong(WAL_MAGIC_PREFIX);
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.putLong(batchSize / POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.put(HEADER_PADDING);
                writeBuffer.flip();

                final MemorySegment batchMemorySegment = ringBufferMemorySegment.asSlice(batchOffset, batchSize);

                writeBuffer.put(
                    batchMemorySegment.asByteBuffer()
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
                long offset = batchOffset;

                writeBuffer.clear();
                writeBuffer.putLong(WAL_MAGIC_PREFIX);
                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.putLong(batchSize / POSTGRES_ENTRY_RECORD_SIZE);
                writeBuffer.put(HEADER_PADDING);

                while (offset < batchOffset + batchSize) {
                    int chunkSize = ledgerConfiguration.wal().writeFileBufferSize() < batchSize
                        ? ledgerConfiguration.wal().writeFileBufferSize()
                        : (int) batchSize;

                    final MemorySegment chunkMemorySegment = ringBufferMemorySegment.asSlice(offset, chunkSize);
                    writeBuffer.put(
                        chunkMemorySegment.asByteBuffer()
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

            this.walFileOffset = 0;

//            WAL_FILE_OFFSET_VAR_HANDLE.set(this, 0);

            log.info("Opened new WAL file: shard={}, file={}", partitionNumber, walFileName);
        } catch (IOException exception) {
            log.error("Error opening new WAL file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void openNewCheckpointFile(String walFilename) {
        try {
            final String checkpointFileName = newCheckpointFilename(walFilename);


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

            this.walFileOffset = 0;

//            walFileOffset.set(0);

            log.info("Opened new checkpoint file: shard={}, file={}", partitionNumber, checkpointFileName);
        } catch (IOException exception) {
            log.error("Error opening new checkpoint file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void rotateFiles() throws IOException {
        log.debug(
            "Rotating WAL file: shard={}, currentSize={}",
            partitionNumber,
            walFileOffset
//            walFileOffset.get()
        );

//        walSequenceId.incrementAndGet();
        walSequenceId++;
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
