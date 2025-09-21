package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import com.sun.nio.file.ExtendedOpenOption;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration.WalConfiguration;

import java.io.IOException;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class PostgresCheckpointWriter {

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
    private FileChannel checkpointFileChannel;
    private volatile boolean isOpen = false;

    public PostgresCheckpointWriter(String path, int shardId) {
        this.shardId = shardId;


        // Open first file
        this.walDirectory = Path.of(path);
        this.writeBuffer = ByteBuffer.allocateDirect(
            CHECKPOINT_PADDING.length
        );
//        this.walFileOffset.set(walFileOffset);
    }

    public void initialize(String filename) {
        createDirectory(this.walDirectory);
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

    private String newCheckpointFilename(String newFilename) {
        return newFilename + "_pg_sequence_id.dat";
    }

    public void writeCheckpoint(long walSequenceId) {
        try {
            writeBuffer.clear();
            writeBuffer.putLong(walSequenceId);
            writeBuffer.put(CHECKPOINT_PADDING);
            writeBuffer.flip();

            long bytesWritten = 0;

            while (writeBuffer.hasRemaining()) {
                bytesWritten += checkpointFileChannel.write(writeBuffer);
            }

        } catch (IOException exception) {
            log.error("Error writing batch to file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void openNewCheckpointFile(String filename) {
        try {
            final String checkpointFileName = newCheckpointFilename(filename);


            final Path filePath = walDirectory.resolve(checkpointFileName);

            // Open с Direct I/O flags
            this.checkpointFileChannel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.APPEND,
                ExtendedOpenOption.DIRECT
            ); // Direct I/O

            this.checkpointFileChannel.force(true);
            this.checkpointFileChannel.position(checkpointFileChannel.size());

            walFileOffset.set(0);

            log.info("Opened new checkpoint file: shard={}, file={}", shardId, checkpointFileName);
        } catch (IOException exception) {
            log.error("Error opening new checkpoint file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void rotateFiles(String filename) throws IOException {
        log.debug(
            "Rotating WAL file: shard={}, currentSize={}",
            shardId,
            checkpointFileChannel.size()
        );

        walFileSequence.incrementAndGet();
        closeCheckpointFile();
        openNewCheckpointFile(filename);
    }

    private void closeCheckpointFile() throws IOException {
        if (checkpointFileChannel != null && checkpointFileChannel.isOpen()) {
            checkpointFileChannel.force(true);
            checkpointFileChannel.close();
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
