package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv7;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-Free Memory-Mapped Reader - минимальные блокировки
 */
@Slf4j
public class LockFreeMemoryMappedReader implements AutoCloseable {
    private final Path segmentDirectory;
    private final AtomicLong segmentSize = new AtomicLong(128L * 1024 * 1024);

    // Lock-free segment management
    private final ConcurrentHashMap<Long, SegmentInfo> segments = new ConcurrentHashMap<>();
    private final AtomicLong nextSegmentId = new AtomicLong(0);

    // Single writer per segment pattern (eliminates locks)
    private final ConcurrentHashMap<Long, AtomicLong> segmentWritePositions = new ConcurrentHashMap<>();

    // Background tasks
    private final ScheduledExecutorService backgroundTasks =
        Executors.newScheduledThreadPool(2, Thread.ofVirtual().name("mm-background-").factory());

    // Segment info with atomic operations
    private static class SegmentInfo {
        final MappedByteBuffer buffer;
        final AtomicLong writePosition;
        final long segmentId;
        volatile boolean readOnly = false;

        SegmentInfo(MappedByteBuffer buffer, long segmentId) {
            this.buffer = buffer;
            this.segmentId = segmentId;
            this.writePosition = new AtomicLong(0);
        }
    }

    public LockFreeMemoryMappedReader(Path segmentDirectory) throws IOException {
        this.segmentDirectory = segmentDirectory;
        initializeSegments();
        startBackgroundTasks();
    }

    private void initializeSegments() throws IOException {
        if (!Files.exists(segmentDirectory)) {
            Files.createDirectories(segmentDirectory);
            log.info("Created segment directory: {}", segmentDirectory);
            return;
        }

        try (var stream = Files.list(segmentDirectory)) {
            List<Path> segmentFiles = stream
                .filter(path -> path.getFileName().toString().startsWith("segment-")
                    && path.getFileName().toString().endsWith(".data"))
                .sorted()
                .toList();

            for (Path segmentFile : segmentFiles) {
                try {
                    String filename = segmentFile.getFileName().toString();
                    String segmentIdStr = filename.substring(8, filename.length() - 5);
                    long segmentId = Long.parseLong(segmentIdStr);

                    FileChannel channel = FileChannel.open(segmentFile,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);

                    long fileSize = channel.size();
                    long mappingSize = Math.max(fileSize, segmentSize.get());

                    if (fileSize < segmentSize.get()) {
                        channel.truncate(segmentSize.get());
                    }

                    MappedByteBuffer buffer = channel.map(
                        FileChannel.MapMode.READ_WRITE, 0, mappingSize);

                    SegmentInfo segmentInfo = new SegmentInfo(buffer, segmentId);
                    segments.put(segmentId, segmentInfo);

                    // Recover write position by scanning
                    recoverSegmentWritePosition(segmentInfo);

                    nextSegmentId.set(Math.max(nextSegmentId.get(), segmentId + 1));

                    log.debug("Loaded segment {} with write position {}",
                        segmentId, segmentInfo.writePosition.get());

                } catch (Exception e) {
                    log.error("Failed to load segment file: {}", segmentFile, e);
                }
            }

            log.info("Initialized {} lock-free memory-mapped segments", segments.size());

            if (segments.isEmpty()) {
                createNewSegment(0L);
            }
        }
    }

    private void recoverSegmentWritePosition(SegmentInfo segmentInfo) {
        // Scan segment to find last valid entry
        MappedByteBuffer buffer = segmentInfo.buffer;
        long position = 0;

        buffer.position(0);
        while (buffer.remaining() >= 8) {
            int entryLength = buffer.getInt();
            if (entryLength <= 0 || entryLength > buffer.remaining()) {
                break; // Invalid entry, stop here
            }

            // Skip entry data
            buffer.position(buffer.position() + entryLength);
            position = buffer.position();
        }

        segmentInfo.writePosition.set(position);
    }

    /**
     * Lock-free update using compare-and-set
     */
    public CompletableFuture<Void> updateAsync(long offset, byte[] data) {
        return CompletableFuture.runAsync(() -> {
            try {
                long segmentId = offset / segmentSize.get();
                SegmentInfo segmentInfo = getOrCreateSegment(segmentId);

                if (segmentInfo.readOnly) {
                    log.warn("Attempting to write to read-only segment {}", segmentId);
                    return;
                }

                // Lock-free append using CAS
                while (true) {
                    long currentPos = segmentInfo.writePosition.get();
                    int entrySize = 4 + data.length; // length prefix + data

                    // Check if there's space
                    if (currentPos + entrySize > segmentSize.get()) {
                        // Mark as read-only and create new segment
                        segmentInfo.readOnly = true;
                        createNewSegment(segmentId + 1);
                        return; // Retry will use new segment
                    }

                    // Try to reserve space atomically
                    if (segmentInfo.writePosition.compareAndSet(currentPos, currentPos + entrySize)) {
                        // We got the space, write data
                        MappedByteBuffer buffer = segmentInfo.buffer;
                        buffer.position((int) currentPos);
                        buffer.putInt(data.length);
                        buffer.put(data);

                        log.debug("Lock-free write to segment {} at position {}", segmentId, currentPos);
                        return;
                    }

                    // CAS failed, retry with new position
                }

            } catch (Exception e) {
                log.error("Failed to update memory-mapped segment", e);
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    /**
     * Lock-free read
     */
    public byte[] read(long offset) {
        try {
            long segmentId = offset / segmentSize.get();
            SegmentInfo segmentInfo = segments.get(segmentId);
            if (segmentInfo == null) return null;

            int segmentOffset = (int) (offset % segmentSize.get());
            MappedByteBuffer buffer = segmentInfo.buffer;

            // Read without synchronization (safe for immutable data after write)
            if (segmentOffset + 4 > buffer.capacity()) return null;

            int length = buffer.getInt(segmentOffset);
            if (length <= 0 || segmentOffset + 4 + length > buffer.capacity()) return null;

            byte[] data = new byte[length];
            for (int i = 0; i < length; i++) {
                data[i] = buffer.get(segmentOffset + 4 + i);
            }

            return data;

        } catch (Exception e) {
            log.debug("Failed to read from memory-mapped segment at offset {}", offset);
            return null;
        }
    }

    private SegmentInfo getOrCreateSegment(long segmentId) throws IOException {
        return segments.computeIfAbsent(segmentId, id -> {
            try {
                return createNewSegment(id);
            } catch (IOException e) {
                throw new RuntimeException("Failed to create segment " + id, e);
            }
        });
    }

    private SegmentInfo createNewSegment(long segmentId) throws IOException {
        Path segmentFile = segmentDirectory.resolve("segment-" + segmentId + ".data");
        FileChannel channel = FileChannel.open(segmentFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE);

        // Pre-allocate segment
        channel.truncate(segmentSize.get());

        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, segmentSize.get());
        SegmentInfo segmentInfo = new SegmentInfo(buffer, segmentId);

        log.info("Created new lock-free segment {} with size {}MB",
            segmentId, segmentSize.get() / (1024 * 1024));

        return segmentInfo;
    }

    private void startBackgroundTasks() {
        // Periodic sync without locks
        backgroundTasks.scheduleWithFixedDelay(() -> {
            segments.values().forEach(segmentInfo -> {
                try {
                    if (!segmentInfo.readOnly) {
                        segmentInfo.buffer.force(); // Sync dirty pages
                    }
                } catch (Exception e) {
                    log.warn("Failed to sync segment {}", segmentInfo.segmentId, e);
                }
            });
        }, 5, 5, TimeUnit.SECONDS);

        log.info("Started lock-free background tasks");
    }

    @Override
    public void close() throws IOException {
        backgroundTasks.shutdown();
        segments.values().forEach(segmentInfo -> segmentInfo.buffer.force());
        log.info("Closed lock-free memory-mapped reader with {} segments", segments.size());
    }
}
