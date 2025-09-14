package org.seleznyov.iyu.kfin.ledger.application.service;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.time.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.io.*;
import java.sql.*;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.dao.EmptyResultDataAccessException;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

// Domain Models (same as before)
public record LedgerEntry(
    UUID id,
    UUID accountId,
    UUID transactionId,
    EntryType entryType,
    long amount,
    Instant createdAt,
    LocalDate operationDate,
    UUID idempotencyKey,
    String currencyCode,
    long entryOrdinal
) {
    public enum EntryType {
        DEBIT, CREDIT
    }
}

public record WalEntry(
    UUID id,
    long sequenceNumber,
    UUID debitAccountId,
    UUID creditAccountId,
    long amount,
    String currencyCode,
    LocalDate operationDate,
    UUID transactionId,
    UUID idempotencyKey,
    WalStatus status,
    Instant createdAt,
    Instant processedAt,
    String errorMessage
) {
    public enum WalStatus {
        PENDING, PROCESSING, PROCESSED, FAILED, RECOVERED
    }
}

// Log Record - Redpanda style binary format
public class LogRecord {
    public static final int HEADER_SIZE = 32;
    public static final byte MAGIC_BYTE = (byte) 0xAC; // Ledger magic byte
    public static final byte VERSION = 1;

    // Record layout:
    // 0-0: magic (1 byte)
    // 1-1: version (1 byte)
    // 2-5: crc32 (4 bytes)
    // 6-9: record_size (4 bytes)
    // 10-17: timestamp (8 bytes)
    // 18-25: offset (8 bytes) - global offset in partition
    // 26-31: reserved (6 bytes)
    // 32+: payload

    private final byte magic;
    private final byte version;
    private final int crc32;
    private final int recordSize;
    private final long timestamp;
    private final long offset;
    private final byte[] payload;

    public LogRecord(long offset, byte[] payload) {
        this.magic = MAGIC_BYTE;
        this.version = VERSION;
        this.recordSize = HEADER_SIZE + payload.length;
        this.timestamp = System.currentTimeMillis();
        this.offset = offset;
        this.payload = payload;
        this.crc32 = calculateCRC32(payload);
    }

    private LogRecord(byte magic, byte version, int crc32, int recordSize,
                      long timestamp, long offset, byte[] payload) {
        this.magic = magic;
        this.version = version;
        this.crc32 = crc32;
        this.recordSize = recordSize;
        this.timestamp = timestamp;
        this.offset = offset;
        this.payload = payload;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.put(magic);
        buffer.put(version);
        buffer.putInt(crc32);
        buffer.putInt(recordSize);
        buffer.putLong(timestamp);
        buffer.putLong(offset);
        buffer.position(buffer.position() + 6); // reserved
        buffer.put(payload);
    }

    public static LogRecord readFrom(ByteBuffer buffer, long expectedOffset) throws IOException {
        if (buffer.remaining() < HEADER_SIZE) {
            throw new IOException("Insufficient data for header");
        }

        int startPosition = buffer.position();

        byte magic = buffer.get();
        if (magic != MAGIC_BYTE) {
            throw new IOException("Invalid magic byte: " + magic);
        }

        byte version = buffer.get();
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        int crc32 = buffer.getInt();
        int recordSize = buffer.getInt();
        long timestamp = buffer.getLong();
        long offset = buffer.getLong();
        buffer.position(buffer.position() + 6); // skip reserved

        if (offset != expectedOffset) {
            throw new IOException("Offset mismatch. Expected: " + expectedOffset + ", got: " + offset);
        }

        int payloadSize = recordSize - HEADER_SIZE;
        if (buffer.remaining() < payloadSize) {
            throw new IOException("Insufficient data for payload");
        }

        byte[] payload = new byte[payloadSize];
        buffer.get(payload);

        // Verify CRC32
        if (calculateCRC32(payload) != crc32) {
            throw new IOException("CRC32 mismatch");
        }

        return new LogRecord(magic, version, crc32, recordSize, timestamp, offset, payload);
    }

    private static int calculateCRC32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    public int size() { return recordSize; }
    public long offset() { return offset; }
    public long timestamp() { return timestamp; }
    public byte[] payload() { return payload; }
}

// Ledger Entry serialization to binary format
public class LedgerEntrySerializer {

    public static byte[] serialize(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        ByteBuffer buffer = ByteBuffer.allocate(512); // Fixed size for performance

        // Transaction header
        buffer.putLong(debitEntry.transactionId().getMostSignificantBits());
        buffer.putLong(debitEntry.transactionId().getLeastSignificantBits());
        buffer.putLong(debitEntry.idempotencyKey().getMostSignificantBits());
        buffer.putLong(debitEntry.idempotencyKey().getLeastSignificantBits());
        buffer.putLong(debitEntry.createdAt().toEpochMilli());
        buffer.putInt((int) debitEntry.operationDate().toEpochDay());

        // Currency (8 bytes)
        byte[] currencyBytes = debitEntry.currencyCode().getBytes(StandardCharsets.UTF_8);
        buffer.put(Arrays.copyOf(currencyBytes, 8));

        // Amount
        buffer.putLong(debitEntry.amount());

        // Debit entry
        buffer.putLong(debitEntry.id().getMostSignificantBits());
        buffer.putLong(debitEntry.id().getLeastSignificantBits());
        buffer.putLong(debitEntry.accountId().getMostSignificantBits());
        buffer.putLong(debitEntry.accountId().getLeastSignificantBits());
        buffer.putLong(debitEntry.entryOrdinal());

        // Credit entry
        buffer.putLong(creditEntry.id().getMostSignificantBits());
        buffer.putLong(creditEntry.id().getLeastSignificantBits());
        buffer.putLong(creditEntry.accountId().getMostSignificantBits());
        buffer.putLong(creditEntry.accountId().getLeastSignificantBits());
        buffer.putLong(creditEntry.entryOrdinal());

        // Padding to fixed size
        while (buffer.position() < 512) {
            buffer.put((byte) 0);
        }

        return buffer.array();
    }

    public static TransactionData deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        // Transaction header
        UUID transactionId = new UUID(buffer.getLong(), buffer.getLong());
        UUID idempotencyKey = new UUID(buffer.getLong(), buffer.getLong());
        Instant createdAt = Instant.ofEpochMilli(buffer.getLong());
        LocalDate operationDate = LocalDate.ofEpochDay(buffer.getInt());

        // Currency
        byte[] currencyBytes = new byte[8];
        buffer.get(currencyBytes);
        String currencyCode = new String(currencyBytes, StandardCharsets.UTF_8).trim().replace("\0", "");

        // Amount
        long amount = buffer.getLong();

        // Debit entry
        UUID debitId = new UUID(buffer.getLong(), buffer.getLong());
        UUID debitAccountId = new UUID(buffer.getLong(), buffer.getLong());
        long debitOrdinal = buffer.getLong();

        // Credit entry
        UUID creditId = new UUID(buffer.getLong(), buffer.getLong());
        UUID creditAccountId = new UUID(buffer.getLong(), buffer.getLong());
        long creditOrdinal = buffer.getLong();

        LedgerEntry debitEntry = new LedgerEntry(
            debitId, debitAccountId, transactionId, LedgerEntry.EntryType.DEBIT,
            amount, createdAt, operationDate, idempotencyKey, currencyCode, debitOrdinal
        );

        LedgerEntry creditEntry = new LedgerEntry(
            creditId, creditAccountId, transactionId, LedgerEntry.EntryType.CREDIT,
            amount, createdAt, operationDate, idempotencyKey, currencyCode, creditOrdinal
        );

        return new TransactionData(debitEntry, creditEntry);
    }

    public static record TransactionData(LedgerEntry debitEntry, LedgerEntry creditEntry) {}
}

// Log Segment - similar to Kafka segment
public class LogSegment implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LogSegment.class);
    private static final int SEGMENT_SIZE = 256 * 1024 * 1024; // 256MB segments

    private final long baseOffset;
    private final Path segmentFile;
    private final Path indexFile;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final SegmentIndex index;

    private volatile long nextOffset;
    private volatile boolean readOnly;

    public LogSegment(Path logDir, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;
        this.segmentFile = logDir.resolve(String.format("%020d.log", baseOffset));
        this.indexFile = logDir.resolve(String.format("%020d.index", baseOffset));

        this.file = new RandomAccessFile(segmentFile.toFile(), "rw");
        this.channel = file.getChannel();

        // Pre-allocate segment file
        file.setLength(SEGMENT_SIZE);
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, SEGMENT_SIZE);

        this.index = new SegmentIndex(indexFile, baseOffset);
        this.nextOffset = baseOffset;

        // Recovery: find actual next offset by scanning
        recoverNextOffset();

        logger.info("Created segment: {} with base offset: {}, next offset: {}",
            segmentFile, baseOffset, nextOffset);
    }

    private void recoverNextOffset() throws IOException {
        ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
        readBuffer.position(0);

        while (readBuffer.hasRemaining()) {
            int position = readBuffer.position();

            try {
                LogRecord record = LogRecord.readFrom(readBuffer, nextOffset);
                nextOffset = record.offset() + 1;

                // Update index every 1000 records
                if ((nextOffset - baseOffset) % 1000 == 0) {
                    index.addEntry(nextOffset, position);
                }

            } catch (IOException e) {
                // End of valid records
                break;
            }
        }

        logger.info("Recovered segment {}: next offset = {}", baseOffset, nextOffset);
    }

    public synchronized long append(byte[] data) throws IOException {
        if (readOnly) {
            throw new IllegalStateException("Segment is read-only");
        }

        LogRecord record = new LogRecord(nextOffset, data);
        int recordSize = record.size();

        // Check if segment has space
        if (buffer.position() + recordSize > SEGMENT_SIZE) {
            throw new IOException("Segment is full");
        }

        // Write record
        record.writeTo(buffer);

        // Force write to disk for durability
        buffer.force();

        // Update index periodically
        if ((nextOffset - baseOffset) % 1000 == 0) {
            index.addEntry(nextOffset, buffer.position() - recordSize);
        }

        long currentOffset = nextOffset;
        nextOffset++;

        return currentOffset;
    }

    public LogRecord read(long offset) throws IOException {
        if (offset < baseOffset || offset >= nextOffset) {
            throw new IllegalArgumentException("Offset out of range: " + offset);
        }

        // Find position using index
        int position = index.findPosition(offset);

        ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
        readBuffer.position(position);

        // Scan from index position to find exact record
        while (readBuffer.hasRemaining()) {
            int recordStart = readBuffer.position();
            try {
                LogRecord record = LogRecord.readFrom(readBuffer, offset);
                if (record.offset() == offset) {
                    return record;
                }
                // Continue scanning if not exact match
            } catch (IOException e) {
                throw new IOException("Failed to read record at offset " + offset, e);
            }
        }

        throw new IOException("Record not found at offset: " + offset);
    }

    public boolean isFull() {
        return buffer.position() + 1024 > SEGMENT_SIZE; // Leave some buffer
    }

    public void makeReadOnly() {
        this.readOnly = true;
        index.close();
    }

    public long baseOffset() { return baseOffset; }
    public long nextOffset() { return nextOffset; }
    public boolean contains(long offset) { return offset >= baseOffset && offset < nextOffset; }

    @Override
    public void close() throws IOException {
        index.close();
        buffer.force();
        channel.close();
        file.close();
    }
}

// Sparse index for fast offset -> file position lookup
public class SegmentIndex implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIndex.class);
    private static final int INDEX_ENTRY_SIZE = 12; // 8 bytes offset + 4 bytes position

    private final Path indexFile;
    private final long baseOffset;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final NavigableMap<Long, Integer> memoryIndex;

    public SegmentIndex(Path indexFile, long baseOffset) throws IOException {
        this.indexFile = indexFile;
        this.baseOffset = baseOffset;
        this.memoryIndex = new ConcurrentSkipListMap<>();

        this.file = new RandomAccessFile(indexFile.toFile(), "rw");
        this.channel = file.getChannel();

        // Pre-allocate index file (expect ~256 entries for 256MB segment)
        int indexSize = 256 * INDEX_ENTRY_SIZE;
        file.setLength(indexSize);
        this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize);

        // Load existing index entries
        loadIndex();
    }

    private void loadIndex() throws IOException {
        ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
        readBuffer.position(0);

        while (readBuffer.hasRemaining()) {
            long offset = readBuffer.getLong();
            if (offset == 0) break; // End of valid entries

            int position = readBuffer.getInt();
            memoryIndex.put(offset, position);
        }

        logger.debug("Loaded {} index entries for base offset {}", memoryIndex.size(), baseOffset);
    }

    public synchronized void addEntry(long offset, int position) {
        memoryIndex.put(offset, position);

        // Write to file
        buffer.putLong(offset);
        buffer.putInt(position);
        buffer.force();
    }

    public int findPosition(long targetOffset) {
        // Find closest offset <= targetOffset
        Map.Entry<Long, Integer> entry = memoryIndex.floorEntry(targetOffset);

        if (entry == null) {
            return 0; // Start from beginning
        }

        return entry.getValue();
    }

    public void close() {
        try {
            buffer.force();
            channel.close();
            file.close();
        } catch (IOException e) {
            logger.error("Failed to close index file: {}", indexFile, e);
        }
    }
}

// Partition - manages multiple segments (like Kafka partition)
public class LedgerPartition {
    private static final Logger logger = LoggerFactory.getLogger(LedgerPartition.class);

    private final int partitionId;
    private final Path partitionDir;
    private final NavigableMap<Long, LogSegment> segments;
    private final AtomicReference<LogSegment> activeSegment;
    private final AtomicLong nextOffset;

    // Single writer thread for this partition
    private final ExecutorService writerExecutor;

    public LedgerPartition(int partitionId, Path baseDir) throws IOException {
        this.partitionId = partitionId;
        this.partitionDir = baseDir.resolve("partition-" + partitionId);
        this.segments = new ConcurrentSkipListMap<>();
        this.activeSegment = new AtomicReference<>();
        this.nextOffset = new AtomicLong(0);

        Files.createDirectories(partitionDir);

        // Single virtual thread for writing to maintain ordering
        this.writerExecutor = Executors.newVirtualThreadPerTaskExecutor();

        // Initialize or recover segments
        recoverPartition();

        logger.info("Initialized partition {} with {} segments, next offset: {}",
            partitionId, segments.size(), nextOffset.get());
    }

    private void recoverPartition() throws IOException {
        // Find all segment files
        try (Stream<Path> files = Files.list(partitionDir)) {
            files.filter(path -> path.toString().endsWith(".log"))
                .sorted()
                .forEach(this::recoverSegment);
        }

        if (segments.isEmpty()) {
            // Create initial segment
            createNewSegment(0);
        } else {
            // Set active segment to the last one
            LogSegment lastSegment = segments.lastEntry().getValue();
            activeSegment.set(lastSegment);
            nextOffset.set(lastSegment.nextOffset());
        }
    }

    private void recoverSegment(Path segmentFile) {
        try {
            String fileName = segmentFile.getFileName().toString();
            long baseOffset = Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));

            LogSegment segment = new LogSegment(partitionDir, baseOffset);
            segments.put(baseOffset, segment);

        } catch (Exception e) {
            logger.error("Failed to recover segment: {}", segmentFile, e);
        }
    }

    private void createNewSegment(long baseOffset) throws IOException {
        LogSegment segment = new LogSegment(partitionDir, baseOffset);

        // Make previous segment read-only
        LogSegment oldActive = activeSegment.getAndSet(segment);
        if (oldActive != null) {
            oldActive.makeReadOnly();
        }

        segments.put(baseOffset, segment);

        logger.info("Created new segment for partition {} with base offset: {}", partitionId, baseOffset);
    }

    public CompletableFuture<Long> append(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                byte[] data = LedgerEntrySerializer.serialize(debitEntry, creditEntry);

                LogSegment current = activeSegment.get();
                if (current.isFull()) {
                    // Create new segment
                    synchronized (this) {
                        current = activeSegment.get();
                        if (current.isFull()) {
                            createNewSegment(nextOffset.get());
                            current = activeSegment.get();
                        }
                    }
                }

                long offset = current.append(data);
                nextOffset.set(offset + 1);

                return offset;

            } catch (IOException e) {
                logger.error("Failed to append to partition {}", partitionId, e);
                throw new RuntimeException("Append failed", e);
            }
        }, writerExecutor);
    }

    public LogRecord read(long offset) throws IOException {
        // Find segment containing this offset
        Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);
        if (entry == null) {
            throw new IllegalArgumentException("Offset too low: " + offset);
        }

        LogSegment segment = entry.getValue();
        if (!segment.contains(offset)) {
            throw new IllegalArgumentException("Offset not found: " + offset);
        }

        return segment.read(offset);
    }

    public long getCurrentOffset() {
        return nextOffset.get();
    }

    public void close() throws IOException {
        writerExecutor.shutdown();
        try {
            if (!writerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                writerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writerExecutor.shutdownNow();
        }

        for (LogSegment segment : segments.values()) {
            segment.close();
        }
    }
}

// Main Ledger Log Manager - Redpanda style
@Component
public class RedpandaLedgerLogManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RedpandaLedgerLogManager.class);
    private static final int DEFAULT_PARTITIONS = 16; // Number of partitions

    @Value("${ledger.log.directory:./ledger-log}")
    private String logDirectory;

    @Value("${ledger.log.partitions:16}")
    private int numPartitions;

    private final Map<Integer, LedgerPartition> partitions;
    private final AtomicLong globalSequence;

    // Disruptor for high-throughput async processing
    private final RingBuffer<LedgerOperationEvent> ringBuffer;
    private final Disruptor<LedgerOperationEvent> disruptor;
    private final ExecutorService disruptorExecutor;

    @Autowired
    private LedgerRepository ledgerRepository;

    public RedpandaLedgerLogManager() {
        this.partitions = new ConcurrentHashMap<>();
        this.globalSequence = new AtomicLong(0);

        // Disruptor for async processing
        this.disruptorExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.disruptor = new Disruptor<>(
            LedgerOperationEvent::new,
            8192, // Ring buffer size
            disruptorExecutor,
            ProducerType.MULTI,
            new BusySpinWaitStrategy() // Low latency
        );

        this.disruptor.handleEventsWith(new LogEventHandler());
        this.ringBuffer = disruptor.getRingBuffer();
    }

    @PostConstruct
    public void initialize() throws IOException {
        // Initialize partitions
        Path logDir = Paths.get(logDirectory);
        Files.createDirectories(logDir);

        for (int i = 0; i < numPartitions; i++) {
            LedgerPartition partition = new LedgerPartition(i, logDir);
            partitions.put(i, partition);

            // Update global sequence to max of all partitions
            long partitionOffset = partition.getCurrentOffset();
            globalSequence.updateAndGet(current -> Math.max(current, partitionOffset));
        }

        // Start disruptor
        disruptor.start();

        logger.info("Initialized Redpanda-style ledger log with {} partitions", numPartitions);
    }

    public CompletableFuture<Long> appendTransaction(LedgerEntry debitEntry, LedgerEntry creditEntry) {
        CompletableFuture<Long> future = new CompletableFuture<>();

        // Publish to disruptor for async processing
        long sequence = ringBuffer.next();
        try {
            LedgerOperationEvent event = ringBuffer.get(sequence);
            event.setDebitEntry(debitEntry);
            event.setCreditEntry(creditEntry);
            event.setCompletionFuture(future);
        } finally {
            ringBuffer.publish(sequence);
        }

        return future;
    }

    private int getPartition(UUID accountId) {
        // Consistent hashing to distribute accounts across partitions
        return Math.abs(accountId.hashCode()) % numPartitions;
    }

    // Event handler for Disruptor
    private class LogEventHandler implements EventHandler<LedgerOperationEvent> {
        private final Map<Integer, List<PendingWrite>> pendingWrites = new HashMap<>();

        @Override
        public void onEvent(LedgerOperationEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                // Choose partition based on debit account (for consistent ordering)
                int partitionId = getPartition(event.getDebitEntry().accountId());

                // Add to pending writes for this partition
                pendingWrites.computeIfAbsent(partitionId, k -> new ArrayList<>())
                    .add(new PendingWrite(event));

                // Flush when batch is full or end of batch
                if (endOfBatch || pendingWrites.get(partitionId).size() >= 100) {
                    flushPartition(partitionId);
                }

            } catch (Exception e) {
                logger.error("Failed to process log event", e);
                event.getCompletionFuture().completeExceptionally(e);
            }
        }

        private void flushPartition(int partitionId) {
            List<PendingWrite> writes = pendingWrites.get(partitionId);
            if (writes == null || writes.isEmpty()) {
                return;
            }

            LedgerPartition partition = partitions.get(partitionId);

            // Process all writes for this partition
            for (PendingWrite write : writes) {
                try {
                    CompletableFuture<Long> appendFuture = partition.append(
                        write.event.getDebitEntry(),
                        write.event.getCreditEntry()
                    );

                    // Chain completion
                    appendFuture.whenComplete((offset, throwable) -> {
                        if (throwable != null) {
                            write.event.getCompletionFuture().completeExceptionally(throwable);
                        } else {
                            write.event.getCompletionFuture().complete(offset);
                        }
                    });

                } catch (Exception e) {
                    write.event.getCompletionFuture().completeExceptionally(e);
                }
            }

            writes.clear();
        }

        private record PendingWrite(LedgerOperationEvent event) {}
    }

    public List<LedgerEntrySerializer.TransactionData> readRange(int partitionId, long fromOffset, long toOffset) throws IOException {
        LedgerPartition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Invalid partition: " + partitionId);
        }

        List<LedgerEntrySerializer.TransactionData> transactions = new ArrayList<>();

        for (long offset = fromOffset; offset < toOffset; offset++) {
            try {
                LogRecord record = partition.read(offset);
                LedgerEntrySerializer.TransactionData transaction =
                    LedgerEntrySerializer.deserialize(record.payload());
                transactions.add(transaction);
            } catch (IOException e) {
                logger.warn("Failed to read offset {}: {}", offset, e.getMessage());
                break;
            }
        }

        return transactions;
    }

    @PreDestroy
    @Override
    public void close() throws IOException {
        logger.info("Shutting down Redpanda-style ledger log");

        // Stop disruptor
        disruptor.halt();

        // Close all partitions
        for (LedgerPartition partition : partitions.values()) {
            partition.close();
        }

        // Shutdown executor
        disruptorExecutor.shutdown();
        try {
            if (!disruptorExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                disruptorExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            disruptorExecutor.shutdownNow();
        }

        logger.info("Redpanda-style ledger log shutdown complete");
    }
}

// Enhanced Disruptor Event
public class LedgerOperationEvent {
    private LedgerEntry debitEntry;
    private LedgerEntry creditEntry;
    private CompletableFuture<Long> completionFuture;

    public LedgerEntry getDebitEntry() { return debitEntry; }
    public void setDebitEntry(LedgerEntry debitEntry) { this.debitEntry = debitEntry; }

    public LedgerEntry getCreditEntry() { return creditEntry; }
    public void setCreditEntry(LedgerEntry creditEntry) { this.creditEntry = creditEntry; }

    public CompletableFuture<Long> getCompletionFuture() { return completionFuture; }
    public void setCompletionFuture(CompletableFuture<Long> future) { this.completionFuture = future; }

    public void reset() {
        this.debitEntry = null;
        this.creditEntry = null;
        this.completionFuture = null;
    }
}

// Repository implementation
@Repository
@Transactional
public class PostgreSQLLedgerRepository implements LedgerRepository {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLLedgerRepository.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedJdbcTemplate;

    @Override
    public void insertBatch(List<LedgerEntry> entries, List<WalEntry> walEntries) {
        try {
            // Insert entry records batch
            String entryInsertSql = """
                INSERT INTO ledger.entry_records 
                (id, account_id, transaction_id, entry_type, amount, created_at, 
                 operation_date, idempotency_key, currency_code, entry_ordinal)
                VALUES (?, ?, ?, ?::ledger.entry_type, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """;

            jdbcTemplate.batchUpdate(entryInsertSql, entries, entries.size(),
                (ps, entry) -> {
                    ps.setObject(1, entry.id());
                    ps.setObject(2, entry.accountId());
                    ps.setObject(3, entry.transactionId());
                    ps.setString(4, entry.entryType().name());
                    ps.setLong(5, entry.amount());
                    ps.setTimestamp(6, Timestamp.from(entry.createdAt()));
                    ps.setDate(7, Date.valueOf(entry.operationDate()));
                    ps.setObject(8, entry.idempotencyKey());
                    ps.setString(9, entry.currencyCode());
                    ps.setLong(10, entry.entryOrdinal());
                });

            // Insert WAL entries batch
            String walInsertSql = """
                INSERT INTO ledger.wal_entries 
                (id, sequence_number, debit_account_id, credit_account_id, amount, 
                 currency_code, operation_date, transaction_id, idempotency_key, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::ledger.wal_status, ?)
                ON CONFLICT (idempotency_key) DO UPDATE SET status = 'PROCESSED'
                """;

            jdbcTemplate.batchUpdate(walInsertSql, walEntries, walEntries.size(),
                (ps, walEntry) -> {
                    ps.setObject(1, walEntry.id());
                    ps.setLong(2, walEntry.sequenceNumber());
                    ps.setObject(3, walEntry.debitAccountId());
                    ps.setObject(4, walEntry.creditAccountId());
                    ps.setLong(5, walEntry.amount());
                    ps.setString(6, walEntry.currencyCode());
                    ps.setDate(7, Date.valueOf(walEntry.operationDate()));
                    ps.setObject(8, walEntry.transactionId());
                    ps.setObject(9, walEntry.idempotencyKey());
                    ps.setString(10, walEntry.status().name());
                    ps.setTimestamp(11, Timestamp.from(walEntry.createdAt()));
                });

            logger.debug("Successfully inserted batch: {} entries, {} WAL entries",
                entries.size(), walEntries.size());

        } catch (Exception e) {
            logger.error("Failed to insert batch", e);
            throw new RuntimeException("Batch insert failed", e);
        }
    }

    @Override
    public Optional<BalanceSnapshot> getLatestSnapshot(UUID accountId, LocalDate operationDate) {
        String sql = """
            SELECT id, account_id, operation_date, balance, last_entry_record_id, 
                   last_entry_ordinal, operations_count, snapshot_ordinal, created_at
            FROM ledger.entries_snapshots 
            WHERE account_id = ? AND operation_date <= ?
            ORDER BY operation_date DESC, snapshot_ordinal DESC 
            LIMIT 1
            """;

        try {
            return jdbcTemplate.queryForObject(sql,
                (rs, rowNum) -> Optional.of(new BalanceSnapshot(
                    UUID.fromString(rs.getString("id")),
                    UUID.fromString(rs.getString("account_id")),
                    rs.getDate("operation_date").toLocalDate(),
                    rs.getLong("balance"),
                    UUID.fromString(rs.getString("last_entry_record_id")),
                    rs.getLong("last_entry_ordinal"),
                    rs.getInt("operations_count"),
                    rs.getLong("snapshot_ordinal"),
                    rs.getTimestamp("created_at").toInstant()
                )),
                accountId, Date.valueOf(operationDate));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<LedgerEntry> getEntriesAfterOrdinal(UUID accountId, long afterOrdinal, LocalDate operationDate) {
        String sql = """
            SELECT id, account_id, transaction_id, entry_type, amount, created_at,
                   operation_date, idempotency_key, currency_code, entry_ordinal
            FROM ledger.entry_records 
            WHERE account_id = ? AND entry_ordinal > ? AND operation_date <= ?
            ORDER BY entry_ordinal ASC
            """;

        return jdbcTemplate.query(sql,
            (rs, rowNum) -> new LedgerEntry(
                UUID.fromString(rs.getString("id")),
                UUID.fromString(rs.getString("account_id")),
                UUID.fromString(rs.getString("transaction_id")),
                LedgerEntry.EntryType.valueOf(rs.getString("entry_type")),
                rs.getLong("amount"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getDate("operation_date").toLocalDate(),
                UUID.fromString(rs.getString("idempotency_key")),
                rs.getString("currency_code"),
                rs.getLong("entry_ordinal")
            ),
            accountId, afterOrdinal, Date.valueOf(operationDate));
    }

    @Override
    public long calculateBalance(UUID accountId, LocalDate operationDate) {
        String sql = """
            WITH account_entries AS (
                SELECT entry_type, amount 
                FROM ledger.entry_records 
                WHERE account_id = ? AND operation_date <= ?
            )
            SELECT COALESCE(
                SUM(CASE WHEN entry_type = 'CREDIT' THEN amount ELSE -amount END), 0
            ) as balance
            FROM account_entries
            """;

        Long result = jdbcTemplate.queryForObject(sql, Long.class, accountId, Date.valueOf(operationDate));
        return result != null ? result : 0L;
    }

    @Override
    public void updateWalEntryStatus(UUID walEntryId, WalEntry.WalStatus status, String errorMessage) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = ?::ledger.wal_status, processed_at = NOW(), error_message = ?
            WHERE id = ?
            """;

        jdbcTemplate.update(sql, status.name(), errorMessage, walEntryId);
    }

    @Override
    public List<WalEntry> getPendingWalEntries(int limit) {
        String sql = """
            SELECT id, sequence_number, debit_account_id, credit_account_id, amount,
                   currency_code, operation_date, transaction_id, idempotency_key, 
                   status, created_at, processed_at, error_message
            FROM ledger.wal_entries 
            WHERE status IN ('PENDING', 'PROCESSING')
            ORDER BY sequence_number ASC
            LIMIT ?
            """;

        return jdbcTemplate.query(sql,
            (rs, rowNum) -> new WalEntry(
                UUID.fromString(rs.getString("id")),
                rs.getLong("sequence_number"),
                UUID.fromString(rs.getString("debit_account_id")),
                UUID.fromString(rs.getString("credit_account_id")),
                rs.getLong("amount"),
                rs.getString("currency_code"),
                rs.getDate("operation_date").toLocalDate(),
                UUID.fromString(rs.getString("transaction_id")),
                UUID.fromString(rs.getString("idempotency_key")),
                WalEntry.WalStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("processed_at") != null ?
                    rs.getTimestamp("processed_at").toInstant() : null,
                rs.getString("error_message")
            ),
            limit);
    }
}

// Recovery Service for Redpanda-style logs
@Component
public class RedpandaLedgerRecoveryService {
    private static final Logger logger = LoggerFactory.getLogger(RedpandaLedgerRecoveryService.class);

    @Autowired
    private RedpandaLedgerLogManager logManager;

    @Autowired
    private LedgerRepository ledgerRepository;

    @Autowired
    private LedgerService ledgerService;

    @EventListener(ApplicationReadyEvent.class)
    public void performRecovery() {
        logger.info("Starting Redpanda-style ledger recovery");

        try {
            // 1. Recover from database WAL entries
            recoverFromDatabase();

            // 2. Replay unprocessed log entries
            replayLogEntries();

            logger.info("Redpanda-style ledger recovery completed successfully");

        } catch (Exception e) {
            logger.error("Recovery failed", e);
            throw new RuntimeException("Recovery failed - system cannot start safely", e);
        }
    }

    private void recoverFromDatabase() {
        logger.info("Recovering pending WAL entries from database");

        List<WalEntry> pendingEntries = ledgerRepository.getPendingWalEntries(10000);

        if (pendingEntries.isEmpty()) {
            logger.info("No pending WAL entries found in database");
            return;
        }

        logger.info("Found {} pending WAL entries, processing recovery", pendingEntries.size());

        for (WalEntry walEntry : pendingEntries) {
            try {
                // Check if transaction was completed
                boolean entriesExist = checkTransactionExists(walEntry.transactionId());

                if (entriesExist) {
                    // Mark as processed
                    ledgerRepository.updateWalEntryStatus(
                        walEntry.id(),
                        WalEntry.WalStatus.PROCESSED,
                        "Recovered - entries exist"
                    );
                } else {
                    // Mark as failed - will be replayed from log
                    ledgerRepository.updateWalEntryStatus(
                        walEntry.id(),
                        WalEntry.WalStatus.FAILED,
                        "Recovery - needs log replay"
                    );
                }

            } catch (Exception e) {
                logger.error("Failed to process WAL entry {}", walEntry.id(), e);
                ledgerRepository.updateWalEntryStatus(
                    walEntry.id(),
                    WalEntry.WalStatus.FAILED,
                    "Recovery error: " + e.getMessage()
                );
            }
        }
    }

    private void replayLogEntries() throws IOException {
        logger.info("Scanning log partitions for replay candidates");

        // Get the last processed offset from database
        long lastProcessedOffset = getLastProcessedOffset();

        // Replay from all partitions
        for (int partitionId = 0; partitionId < logManager.numPartitions; partitionId++) {
            replayPartition(partitionId, lastProcessedOffset);
        }
    }

    private void replayPartition(int partitionId, long fromOffset) throws IOException {
        logger.info("Replaying partition {} from offset {}", partitionId, fromOffset);

        try {
            // Read transactions from log that may not be in database
            List<LedgerEntrySerializer.TransactionData> transactions =
                logManager.readRange(partitionId, fromOffset, Long.MAX_VALUE);

            int replayedCount = 0;

            for (LedgerEntrySerializer.TransactionData transaction : transactions) {
                try {
                    // Check if this transaction exists in database
                    if (!checkTransactionExists(transaction.debitEntry().transactionId())) {
                        // Replay transaction
                        CompletableFuture<Void> future = ledgerService.transferWithIdempotency(
                            transaction.debitEntry().accountId(),
                            transaction.creditEntry().accountId(),
                            transaction.debitEntry().amount(),
                            transaction.debitEntry().currencyCode(),
                            transaction.debitEntry().idempotencyKey()
                        );

                        // Wait for completion
                        future.get(30, TimeUnit.SECONDS);
                        replayedCount++;

                        logger.debug("Replayed transaction: {}", transaction.debitEntry().transactionId());
                    }

                } catch (Exception e) {
                    logger.error("Failed to replay transaction: {}",
                        transaction.debitEntry().transactionId(), e);
                }
            }

            logger.info("Replayed {} transactions from partition {}", replayedCount, partitionId);

        } catch (IOException e) {
            logger.error("Failed to read from partition {}: {}", partitionId, e.getMessage());
        }
    }

    private boolean checkTransactionExists(UUID transactionId) {
        try {
            String sql = "SELECT COUNT(*) FROM ledger.entry_records WHERE transaction_id = ?";
            Integer count = ledgerRepository.jdbcTemplate.queryForObject(sql, Integer.class, transactionId);
            return count != null && count >= 2; // Should have both debit and credit
        } catch (Exception e) {
            logger.error("Failed to check transaction existence: {}", transactionId, e);
            return false;
        }
    }

    private long getLastProcessedOffset() {
        try {
            String sql = """
                SELECT COALESCE(MAX(entry_ordinal), 0) 
                FROM ledger.entry_records
                """;
            Long result = ledgerRepository.jdbcTemplate.queryForObject(sql, Long.class);
            return result != null ? result : 0L;
        } catch (Exception e) {
            logger.error("Failed to get last processed offset", e);
            return 0L;
        }
    }
}

// High-level Ledger Service
@Service
public class LedgerService {
    private static final Logger logger = LoggerFactory.getLogger(LedgerService.class);

    @Autowired
    private RedpandaLedgerLogManager logManager;

    @Autowired
    private LedgerRepository ledgerRepository;

    // Async batch processor for database writes
    private final ScheduledExecutorService batchProcessor;
    private final ConcurrentLinkedQueue<CompletedTransaction> pendingDbWrites;

    public LedgerService() {
        this.batchProcessor = Executors.newScheduledThreadPool(1);
        this.pendingDbWrites = new ConcurrentLinkedQueue<>();

        // Process database writes every 50ms
        batchProcessor.scheduleAtFixedRate(this::processDatabaseBatch, 50, 50, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Void> transfer(
        UUID fromAccountId,
        UUID toAccountId,
        long amount,
        String currencyCode) {

        return transferWithIdempotency(fromAccountId, toAccountId, amount, currencyCode, UUID.randomUUID());
    }

    public CompletableFuture<Void> transferWithIdempotency(
        UUID fromAccountId,
        UUID toAccountId,
        long amount,
        String currencyCode,
        UUID idempotencyKey) {

        // Validate inputs
        if (amount <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Amount must be positive"));
        }
        if (fromAccountId.equals(toAccountId)) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Cannot transfer to same account"));
        }

        // Check if operation already exists
        if (isOperationProcessed(idempotencyKey)) {
            return CompletableFuture.completedFuture(null);
        }

        UUID transactionId = generateUUIDv7();
        Instant now = Instant.now();
        LocalDate operationDate = LocalDate.now();

        // Create ledger entries
        LedgerEntry debitEntry = new LedgerEntry(
            generateUUIDv7(),
            fromAccountId,
            transactionId,
            LedgerEntry.EntryType.DEBIT,
            amount,
            now,
            operationDate,
            idempotencyKey,
            currencyCode,
            0 // Will be set by log manager
        );

        LedgerEntry creditEntry = new LedgerEntry(
            generateUUIDv7(),
            toAccountId,
            transactionId,
            LedgerEntry.EntryType.CREDIT,
            amount,
            now,
            operationDate,
            idempotencyKey,
            currencyCode,
            0 // Will be set by log manager
        );

        // Append to log first (durability)
        CompletableFuture<Long> logFuture = logManager.appendTransaction(debitEntry, creditEntry);

        // Chain database write after log write
        return logFuture.thenCompose(offset -> {
            // Queue for database batch write
            pendingDbWrites.offer(new CompletedTransaction(debitEntry, creditEntry, offset));
            return CompletableFuture.completedFuture(null);
        });
    }

    private void processDatabaseBatch() {
        List<CompletedTransaction> batch = new ArrayList<>();
        CompletedTransaction transaction;

        // Collect up to 1000 transactions for batch
        while ((transaction = pendingDbWrites.poll()) != null && batch.size() < 1000) {
            batch.add(transaction);
        }

        if (batch.isEmpty()) {
            return;
        }

        try {
            // Prepare data for batch insert
            List<LedgerEntry> entries = new ArrayList<>();
            List<WalEntry> walEntries = new ArrayList<>();

            for (CompletedTransaction txn : batch) {
                entries.add(txn.debitEntry);
                entries.add(txn.creditEntry);

                // Create WAL entry for tracking
                WalEntry walEntry = new WalEntry(
                    generateUUIDv7(),
                    txn.logOffset,
                    txn.debitEntry.accountId(),
                    txn.creditEntry.accountId(),
                    txn.debitEntry.amount(),
                    txn.debitEntry.currencyCode(),
                    txn.debitEntry.operationDate(),
                    txn.debitEntry.transactionId(),
                    txn.debitEntry.idempotencyKey(),
                    WalEntry.WalStatus.PROCESSED,
                    Instant.now(),
                    Instant.now(),
                    null
                );
                walEntries.add(walEntry);
            }

            // Batch insert to database
            ledgerRepository.insertBatch(entries, walEntries);

            logger.debug("Processed database batch of {} transactions", batch.size());

        } catch (Exception e) {
            logger.error("Failed to process database batch", e);

            // Re-queue failed transactions (with limit to prevent infinite retry)
            for (CompletedTransaction txn : batch) {
                if (txn.retryCount < 3) {
                    txn.retryCount++;
                    pendingDbWrites.offer(txn);
                } else {
                    logger.error("Dropping transaction after max retries: {}", txn.debitEntry.transactionId());
                }
            }
        }
    }

    public long getAccountBalance(UUID accountId, LocalDate asOfDate) {
        // Try to get latest snapshot
        Optional<BalanceSnapshot> snapshot = ledgerRepository.getLatestSnapshot(accountId, asOfDate);

        long balance = 0;
        long afterOrdinal = 0;

        if (snapshot.isPresent()) {
            balance = snapshot.get().balance();
            afterOrdinal = snapshot.get().lastEntryOrdinal();
        }

        // Add entries after snapshot
        List<LedgerEntry> entries = ledgerRepository.getEntriesAfterOrdinal(accountId, afterOrdinal, asOfDate);

        for (LedgerEntry entry : entries) {
            if (entry.entryType() == LedgerEntry.EntryType.CREDIT) {
                balance += entry.amount();
            } else {
                balance -= entry.amount();
            }
        }

        return balance;
    }

    public long getCurrentBalance(UUID accountId) {
        return getAccountBalance(accountId, LocalDate.now());
    }

    private boolean isOperationProcessed(UUID idempotencyKey) {
        try {
            String sql = "SELECT COUNT(*) FROM ledger.entry_records WHERE idempotency_key = ?";
            Integer count = ledgerRepository.jdbcTemplate.queryForObject(sql, Integer.class, idempotencyKey);
            return count != null && count > 0;
        } catch (Exception e) {
            logger.error("Failed to check operation idempotency", e);
            return false;
        }
    }

    private UUID generateUUIDv7() {
        // UUID v7 implementation
        long timestamp = System.currentTimeMillis();
        long timestampHigh = (timestamp >> 16) & 0xFFFFFFFFL;
        long timestampLow = timestamp & 0xFFFFL;

        long randA = ThreadLocalRandom.current().nextLong(0, 4096);
        long randB = ThreadLocalRandom.current().nextLong();

        long mostSigBits = (timestampHigh << 32) | (timestampLow << 16) | (7L << 12) | (randA & 0xFFF);
        long leastSigBits = (2L << 62) | (randB & 0x3FFFFFFFFFFFFFFFL);

        return new UUID(mostSigBits, leastSigBits);
    }

    private static class CompletedTransaction {
        final LedgerEntry debitEntry;
        final LedgerEntry creditEntry;
        final long logOffset;
        int retryCount = 0;

        CompletedTransaction(LedgerEntry debitEntry, LedgerEntry creditEntry, long logOffset) {
            this.debitEntry = debitEntry;
            this.creditEntry = creditEntry;
            this.logOffset = logOffset;
        }
    }
}