package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv7;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    // Ring buffer для pending writes (lock-free)
    private final RingBuffer<WriteRequest> writeRequestBuffer;
    private final EventHandler<WriteRequest> writeHandler;
    private final Disruptor<WriteRequest> writeDisruptor;

    public LockFreeImmediateWalWriter(Path walDirectory) throws IOException {
        this.walDirectory = walDirectory;
        this.walChannel = FileChannel.open(
            walDirectory.resolve("immediate.wal"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        );

        // Initialize lock-free write pipeline
        this.writeDisruptor = new Disruptor<>(
            WriteRequest::new,
            1024, // Small ring buffer for WAL
            Thread.ofVirtual().name("wal-writer-").factory(),
            ProducerType.MULTI,
            new YieldingWaitStrategy()
        );

        this.writeHandler = new LockFreeWriteHandler();
        writeDisruptor.handleEventsWith(writeHandler);
        writeDisruptor.start();
        this.writeRequestBuffer = writeDisruptor.getRingBuffer();

        log.info("Initialized Lock-Free WAL Writer at {}", walDirectory);
    }

    public long writeImmediate(byte[] data, DurabilityLevel level) throws IOException {
        // Thread-local buffer eliminates synchronization
        ByteBuffer buffer = threadLocalBuffers.get();
        buffer.clear();

        // Prepare write request
        long requestedPosition = position.getAndAdd(data.length + 8);

        // Submit to lock-free ring buffer
        long sequence = writeRequestBuffer.next();
        try {
            WriteRequest request = writeRequestBuffer.get(sequence);
            request.setData(data);
            request.setPosition(requestedPosition);
            request.setDurabilityLevel(level);
            request.setCompletionFuture(new CompletableFuture<>());
        } finally {
            writeRequestBuffer.publish(sequence);
        }

        return requestedPosition;
    }

    /**
     * Lock-free write handler
     */
    private class LockFreeWriteHandler implements EventHandler<WriteRequest> {
        private final ByteBuffer batchBuffer = ByteBuffer.allocateDirect(1024 * 1024); // 1MB
        private final List<WriteRequest> pendingRequests = new ArrayList<>();

        @Override
        public void onEvent(WriteRequest request, long sequence, boolean endOfBatch) throws Exception {
            // Accumulate requests for batch processing
            pendingRequests.add(request);

            if (endOfBatch || pendingRequests.size() >= 100) {
                // Process batch without locks
                processBatch();
            }
        }

        private void processBatch() throws IOException {
            if (pendingRequests.isEmpty()) return;

            batchBuffer.clear();

            // Build batch buffer
            for (WriteRequest request : pendingRequests) {
                batchBuffer.putInt(request.getData().length);
                batchBuffer.put(request.getData());
                batchBuffer.putInt(checksum(request.getData()));
            }

            batchBuffer.flip();

            // Single write call for entire batch
            while (batchBuffer.hasRemaining()) {
                walChannel.write(batchBuffer);
            }

            // Apply durability level
            DurabilityLevel maxLevel = pendingRequests.stream()
                .map(WriteRequest::getDurabilityLevel)
                .max(Comparator.comparing(DurabilityLevel::ordinal))
                .orElse(DurabilityLevel.OS_CACHE_ONLY);

            switch (maxLevel) {
                case IMMEDIATE_FSYNC:
                    walChannel.force(true);
                    break;
                case BATCH_FSYNC:
                    BatchFsyncScheduler.getInstance().schedule(() -> {
                        try { walChannel.force(true); } catch (IOException e) { /* handle */ }
                    });
                    break;
                case ASYNC_FSYNC:
                    // Background fsync in virtual thread
                    CompletableFuture.runAsync(() -> {
                        try { walChannel.force(true); } catch (IOException e) { /* handle */ }
                    });
                    break;
                case OS_CACHE_ONLY:
                    // No explicit sync
                    break;
            }

            // Complete all futures
            pendingRequests.forEach(req -> req.getCompletionFuture().complete(null));
            pendingRequests.clear();
        }
    }

    // Write request для ring buffer
    private static class WriteRequest {
        private byte[] data;
        private long position;
        private DurabilityLevel durabilityLevel;
        private CompletableFuture<Void> completionFuture;

        public WriteRequest() {
            // Default constructor для EventFactory
        }

        // Getters and setters
        public byte[] getData() { return data; }
        public void setData(byte[] data) { this.data = data; }
        public long getPosition() { return position; }
        public void setPosition(long position) { this.position = position; }
        public DurabilityLevel getDurabilityLevel() { return durabilityLevel; }
        public void setDurabilityLevel(DurabilityLevel level) { this.durabilityLevel = level; }
        public CompletableFuture<Void> getCompletionFuture() { return completionFuture; }
        public void setCompletionFuture(CompletableFuture<Void> future) { this.completionFuture = future; }
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
        writeDisruptor.shutdown();
        walChannel.force(true);
        walChannel.close();
    }
}
