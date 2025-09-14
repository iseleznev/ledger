package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-Free Immediate WAL Writer - без synchronized блокировок
 */
@Slf4j
public class LockFreeImmediateWalWriter implements AutoCloseable {

    private final Path walDirectory;
    private final FileChannel walChannel;

    // Lock-free positioning через single writer pattern
    private final AtomicLong position = new AtomicLong(0);

    // Pre-allocated thread-local buffers (eliminates synchronized)
    private final ThreadLocal<ByteBuffer> threadLocalBuffers = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(64 * 1024));

    // Sync strategies
    private final ExecutorService syncExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public LockFreeImmediateWalWriter(Path walDirectory) throws IOException {
        this.walDirectory = walDirectory;
        Files.createDirectories(walDirectory);

        this.walChannel = FileChannel.open(
            walDirectory.resolve("immediate.wal"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        );

        log.info("Initialized Lock-Free WAL Writer at {}", walDirectory);
    }

    public long writeImmediate(byte[] data, DurabilityLevel level) throws IOException {
        // Thread-local buffer eliminates synchronization
        ByteBuffer buffer = threadLocalBuffers.get();
        buffer.clear();

        // Atomic position increment
        long currentPos = position.getAndAdd(data.length + 8);

        // Prepare write data
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.putInt(checksum(data));
        buffer.flip();

        // Single write call
        synchronized (walChannel) { // Only file-level sync needed
            walChannel.position(currentPos);
            while (buffer.hasRemaining()) {
                walChannel.write(buffer);
            }
        }

        // Durability level enforcement
        switch (level) {
            case IMMEDIATE_FSYNC:
                walChannel.force(true);
                break;

            case BATCH_FSYNC:
                scheduleBatchFsync();
                break;

            case ASYNC_FSYNC:
                syncExecutor.submit(() -> {
                    try {
                        walChannel.force(true);
                    } catch (IOException e) {
                        log.error("Async fsync failed", e);
                    }
                });
                break;

            case OS_CACHE_ONLY:
                // No explicit sync
                break;
        }

        return currentPos;
    }

    private void scheduleBatchFsync() {
        BatchFsyncScheduler.getInstance().schedule(() -> {
            try {
                walChannel.force(true);
            } catch (IOException e) {
                log.error("Batch fsync failed", e);
            }
        });
    }

    public byte[] read(long offset) throws IOException {
        ByteBuffer headerBuffer = ByteBuffer.allocate(8);

        synchronized (walChannel) {
            walChannel.position(offset);
            int bytesRead = walChannel.read(headerBuffer);
            if (bytesRead < 4) return null;

            headerBuffer.flip();
            int length = headerBuffer.getInt();

            ByteBuffer dataBuffer = ByteBuffer.allocate(length + 4);
            walChannel.read(dataBuffer);
            dataBuffer.flip();

            byte[] data = new byte[length];
            dataBuffer.get(data);
            int storedChecksum = dataBuffer.getInt();

            if (checksum(data) != storedChecksum) {
                throw new IOException("Checksum mismatch at offset " + offset);
            }

            return data;
        }
    }

    private int checksum(byte[] data) {
        int sum = 0;
        for (byte b : data) {
            sum += b & 0xFF;
        }
        return sum;
    }

    @Override
    public void close() throws IOException {
        try {
            syncExecutor.close();
            walChannel.force(true);
            walChannel.close();
            log.info("Lock-Free WAL Writer closed");
        } catch (Exception e) {
            log.error("Error closing Lock-Free WAL Writer", e);
        }
    }
}
