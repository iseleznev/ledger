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
public class WalBatchCommitsCheckpointWriter {

    private final static int CPU_CACHE_LINE_SIZE = 64;

    //LEDGERPX
    private static final long WAL_MAGIC = ByteBuffer.wrap(
        new byte[] {76, 69, 68, 71, 69, 82, 80, 88}
    ).getLong();

//    private static final long WAL_MAGIC_PREFIX_OFFSET = 0;
//    private static final ValueLayout WAL_MAGIC_TYPE = ValueLayout.JAVA_LONG;
//
    private static final long CHECKPOINT_SEQUENCE_ID_OFFSET = 0;
    private static final ValueLayout CHECKPOINT_SEQUENCE_ID_TYPE = ValueLayout.JAVA_LONG;

    private static final long CHECKPOINT_ENTRIES_COUNT_OFFSET = CHECKPOINT_SEQUENCE_ID_OFFSET + CHECKPOINT_SEQUENCE_ID_TYPE.byteSize();
    private static final ValueLayout CHECKPOINT_ENTRIES_COUNT_TYPE = ValueLayout.JAVA_LONG;

    private static final int CHECKPOINT_RAW_SIZE = (int) (CHECKPOINT_ENTRIES_COUNT_OFFSET + CHECKPOINT_ENTRIES_COUNT_TYPE.byteSize());
    private static final int CHECKPOINT_SIZE = (CHECKPOINT_RAW_SIZE + CPU_CACHE_LINE_SIZE - 1) & -CPU_CACHE_LINE_SIZE;
    private static final byte[] CHECKPOINT_PADDING = new byte[64 - CHECKPOINT_SIZE];

    private final Path walDirectory;
    private final int shardId;
    private final ByteBuffer writeBuffer;
    private FileChannel checkpointFileChannel;

    public WalBatchCommitsCheckpointWriter(
        WalConfiguration configuration,
        int shardId
    ) {
        this.shardId = shardId;


        // Open first file
        final Path walDirectory = Path.of(configuration.path());

        this.walDirectory = walDirectory;
        this.writeBuffer = ByteBuffer.allocateDirect(CHECKPOINT_RAW_SIZE);
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
        return newFilename + "_commits_checkpoint.dat";
    }

    public void closeFile() {
        try {
            if (checkpointFileChannel != null && checkpointFileChannel.isOpen()) {
                checkpointFileChannel.force(true);
                checkpointFileChannel.close();
            }
        } catch (IOException exception) {
            log.error("Error closing file channel", exception);
            throw new RuntimeException(exception);
        }
    }


    public long writeCheckpoint(long walSequenceId, long entryRecordsCount) {
        try {

            writeBuffer.clear();

                writeBuffer.putLong(walSequenceId);
                writeBuffer.putLong(entryRecordsCount);
//                writeBuffer.put(CHECKPOINT_PADDING);
                writeBuffer.flip();

                long bytesWritten = 0;
                while (writeBuffer.hasRemaining()) {
                    bytesWritten += checkpointFileChannel.write(writeBuffer);
                }

                return bytesWritten;

        } catch (IOException exception) {
            log.error("Error writing batch to file", exception);
            throw new RuntimeException(exception);
        }
    }

    private void openNewCheckpointFile(String checkpointFileName) {
        try {
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

            log.info("Opened new checkpoint file: shard={}, file={}", shardId, checkpointFileName);
        } catch (IOException exception) {
            log.error("Error opening new checkpoint file", exception);
            throw new RuntimeException(exception);
        }
    }

    public void rotateFiles(String filename) throws IOException {
        log.debug(
            "Rotating commits checkpoint file: shard={}",
            shardId
        );

        closeCheckpointFile();
        openNewCheckpointFile(filename);
    }

    private void closeCheckpointFile() throws IOException {
        if (checkpointFileChannel != null && checkpointFileChannel.isOpen()) {
            checkpointFileChannel.force(true);
            checkpointFileChannel.close();
        }
    }
}
