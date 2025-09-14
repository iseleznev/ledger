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
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// Sparse Index for fast offset -> file position lookup
public class SegmentIndex implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIndex.class);
    private static final int INDEX_ENTRY_SIZE = 16; // 8 bytes offset + 8 bytes position

    private final Path indexFile;
    private final long baseOffset;
    private final RandomAccessFile file;
    private final FileChannel channel;
    private final MappedByteBuffer buffer;
    private final NavigableMap<Long, Long> memoryIndex;

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

            long position = readBuffer.getLong();
            memoryIndex.put(offset, position);
        }

        logger.debug("Loaded {} index entries for base offset {}", memoryIndex.size(), baseOffset);
    }

    public synchronized void addEntry(long offset, long position) {
        memoryIndex.put(offset, position);

        // Write to file
        buffer.putLong(offset);
        buffer.putLong(position);
        buffer.force();
    }

    public long findPosition(long targetOffset) {
        // Find closest offset <= targetOffset
        Map.Entry<Long, Long> entry = memoryIndex.floorEntry(targetOffset);

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

// Enhanced Log Segment - manages one segment file
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

    public synchronized long append(byte[] data, TransferErrorType errorType) throws IOException {
        if (readOnly) {
            throw new IllegalStateException("Segment is read-only");
        }

        LogRecord record = new LogRecord(nextOffset, data, errorType);
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
        long position = index.findPosition(offset);

        ByteBuffer readBuffer = buffer.asReadOnlyBuffer();
        readBuffer.position((int) position);

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

// Checkpoint Information for recovery
@JsonIgnoreProperties(ignoreUnknown = true)
public static class PartitionCheckpoint {
    private long checkpointTime = System.currentTimeMillis();
    private Map<Long, SegmentCheckpoint> segments = new HashMap<>();
    private long lastOffset = 0;
    private long totalOperations = 0;

    public PartitionCheckpoint() {}

    public void addSegment(long baseOffset, long nextOffset, long operations) {
        segments.put(baseOffset, new SegmentCheckpoint(baseOffset, nextOffset, operations));
        lastOffset = Math.max(lastOffset, nextOffset);
        totalOperations += operations;
    }

    // Getters and setters for Jackson
    public long getCheckpointTime() { return checkpointTime; }
    public void setCheckpointTime(long checkpointTime) { this.checkpointTime = checkpointTime; }

    public Map<Long, SegmentCheckpoint> getSegments() { return segments; }
    public void setSegments(Map<Long, SegmentCheckpoint> segments) { this.segments = segments; }

    public long getLastOffset() { return lastOffset; }
    public void setLastOffset(long lastOffset) { this.lastOffset = lastOffset; }

    public long getTotalOperations() { return totalOperations; }
    public void setTotalOperations(long totalOperations) { this.totalOperations = totalOperations; }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SegmentCheckpoint {
        private long baseOffset;
        private long nextOffset;
        private long operations;

        public SegmentCheckpoint() {}

        public SegmentCheckpoint(long baseOffset, long nextOffset, long operations) {
            this.baseOffset = baseOffset;
            this.nextOffset = nextOffset;
            this.operations = operations;
        }

        // Getters and setters
        public long getBaseOffset() { return baseOffset; }
        public void setBaseOffset(long baseOffset) { this.baseOffset = baseOffset; }

        public long getNextOffset() { return nextOffset; }
        public void setNextOffset(long nextOffset) { this.nextOffset = nextOffset; }

        public long getOperations() { return operations; }
        public void setOperations(long operations) { this.operations = operations; }
    }
}

// Base LedgerPartition class - manages multiple segments (like Kafka partition)
public class LedgerPartition implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LedgerPartition.class);

    protected final int partitionId;
    protected final Path partitionDir;
    protected final NavigableMap<Long, LogSegment> segments;
    protected final AtomicReference<LogSegment> activeSegment;
    protected final AtomicLong nextOffset;
    protected final AtomicLong totalOperations;

    // Single writer thread for this partition
    protected final ExecutorService writerExecutor;
    protected final ObjectMapper objectMapper;

    public LedgerPartition(int partitionId, Path baseDir) throws IOException {
        this.partitionId = partitionId;
        this.partitionDir = baseDir.resolve("partition-" + partitionId);
        this.segments = new ConcurrentSkipListMap<>();
        this.activeSegment = new AtomicReference<>();
        this.nextOffset = new AtomicLong(0);
        this.totalOperations = new AtomicLong(0);

        Files.createDirectories(partitionDir);

        // Single virtual thread for writing to maintain ordering
        this.writerExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.objectMapper = new ObjectMapper();

        // Initialize or recover segments
        recoverPartition();

        logger.info("Initialized partition {} with {} segments, next offset: {}",
            partitionId, segments.size(), nextOffset.get());
    }

    private void recoverPartition() throws IOException {
        // Load checkpoint first
        PartitionCheckpoint checkpoint = loadCheckpoint();

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

        // Validate against checkpoint
        if (checkpoint != null) {
            validateRecoveryAgainstCheckpoint(checkpoint);
        }
    }

    private void recoverSegment(Path segmentFile) {
        try {
            String fileName = segmentFile.getFileName().toString();
            long baseOffset = Long.parseLong(fileName.substring(0, fileName.lastIndexOf('.')));

            LogSegment segment = new LogSegment(partitionDir, baseOffset);
            segments.put(baseOffset, segment);

            // Count operations in this segment
            long operations = segment.nextOffset() - segment.baseOffset();
            totalOperations.addAndGet(operations);

        } catch (Exception e) {
            logger.error("Failed to recover segment: {}", segmentFile, e);
        }
    }

    private void validateRecoveryAgainstCheckpoint(PartitionCheckpoint checkpoint) {
        long recoveredOffset = nextOffset.get();
        long checkpointOffset = checkpoint.getLastOffset();

        if (recoveredOffset < checkpointOffset) {
            logger.warn("Recovery offset {} is behind checkpoint offset {} for partition {}",
                recoveredOffset, checkpointOffset, partitionId);
        } else if (recoveredOffset > checkpointOffset) {
            logger.info("Recovered {} new operations since last checkpoint for partition {}",
                recoveredOffset - checkpointOffset, partitionId);
        } else {
            logger.info("Recovery matches checkpoint for partition {}", partitionId);
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

                long offset = current.append(data, TransferErrorType.BUSINESS_ERROR);
                nextOffset.set(offset + 1);
                totalOperations.incrementAndGet();

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

    public List<LedgerEntrySerializer.TransactionData> readRange(long fromOffset, long toOffset) throws IOException {
        List<LedgerEntrySerializer.TransactionData> transactions = new ArrayList<>();

        for (long offset = fromOffset; offset < Math.min(toOffset, nextOffset.get()); offset++) {
            try {
                LogRecord record = read(offset);
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

    // Checkpoint management
    public void createCheckpoint() {
        try {
            PartitionCheckpoint checkpoint = new PartitionCheckpoint();

            // Collect current state of all segments
            for (Map.Entry<Long, LogSegment> entry : segments.entrySet()) {
                LogSegment segment = entry.getValue();
                long operations = segment.nextOffset() - segment.baseOffset();
                checkpoint.addSegment(segment.baseOffset(), segment.nextOffset(), operations);
            }

            // Save checkpoint atomically
            saveCheckpointAtomic(checkpoint);

            logger.info("Created checkpoint for partition {}: {} segments, {} operations",
                partitionId, segments.size(), totalOperations.get());

        } catch (Exception e) {
            logger.error("Failed to create checkpoint for partition {}", partitionId, e);
        }
    }

    private PartitionCheckpoint loadCheckpoint() {
        try {
            Path checkpointFile = partitionDir.resolve("checkpoint.json");
            if (!Files.exists(checkpointFile)) {
                logger.info("No checkpoint file found for partition {}", partitionId);
                return null;
            }

            PartitionCheckpoint checkpoint = objectMapper.readValue(checkpointFile.toFile(), PartitionCheckpoint.class);
            logger.info("Loaded checkpoint for partition {} from {}", partitionId,
                Instant.ofEpochMilli(checkpoint.getCheckpointTime()));
            return checkpoint;

        } catch (IOException e) {
            logger.warn("Failed to load checkpoint for partition {}: {}", partitionId, e.getMessage());
            return null;
        }
    }

    private void saveCheckpointAtomic(PartitionCheckpoint checkpoint) throws IOException {
        Path tempFile = partitionDir.resolve("checkpoint.json.tmp");
        Path finalFile = partitionDir.resolve("checkpoint.json");

        // Write to temporary file first
        objectMapper.writeValue(tempFile.toFile(), checkpoint);

        // Atomic rename
        Files.move(tempFile, finalFile, StandardCopyOption.REPLACE_EXISTING);
    }

    // Getters for monitoring and management
    public long getCurrentOffset() {
        return nextOffset.get();
    }

    public long getTotalOperations() {
        return totalOperations.get();
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getSegmentCount() {
        return segments.size();
    }

    public boolean isHealthy() {
        return activeSegment.get() != null && !writerExecutor.isShutdown();
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing partition {}", partitionId);

        // Create final checkpoint
        createCheckpoint();

        // Shutdown writer executor
        writerExecutor.shutdown();
        try {
            if (!writerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                writerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            writerExecutor.shutdownNow();
        }

        // Close all segments
        for (LogSegment segment : segments.values()) {
            segment.close();
        }

        logger.info("Partition {} closed successfully", partitionId);
    }
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

// Recovery Service for the entire ledger system
@Component
public class LedgerRecoveryService {
    private static final Logger logger = LoggerFactory.getLogger(LedgerRecoveryService.class);

    @Autowired
    private HybridLedgerManager ledgerManager;

    @Autowired
    private LedgerRepository ledgerRepository;

    @EventListener(ApplicationReadyEvent.class)
    public void performSystemRecovery() {
        logger.info("Starting system-wide ledger recovery");

        try {
            // 1. Recover from database WAL entries
            recoverFromDatabase();

            // 2. Replay unprocessed log entries from all partitions
            replayLogEntries();

            // 3. Validate consistency
            validateSystemConsistency();

            logger.info("System-wide ledger recovery completed successfully");

        } catch (Exception e) {
            logger.error("System recovery failed", e);
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
                    // Mark for replay
                    ledgerRepository.updateWalEntryStatus(
                        walEntry.id(),
                        WalEntry.WalStatus.FAILED,
                        "Recovery - needs replay"
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
        Map<Integer, EnhancedLedgerPartition> partitions = ledgerManager.getPartitions();

        for (Map.Entry<Integer, EnhancedLedgerPartition> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            EnhancedLedgerPartition partition = entry.getValue();

            replayPartition(partitionId, partition, lastProcessedOffset);
        }
    }

    private void replayPartition(int partitionId, EnhancedLedgerPartition partition, long fromOffset) throws IOException {
        logger.info("Replaying partition {} from offset {}", partitionId, fromOffset);

        try {
            List<LedgerEntrySerializer.TransactionData> transactions =
                partition.readRange(fromOffset, Long.MAX_VALUE);

            int replayedCount = 0;

            for (LedgerEntrySerializer.TransactionData transaction : transactions) {
                try {
                    // Check if this transaction exists in database
                    if (!checkTransactionExists(transaction.debitEntry().transactionId())) {
                        // Replay transaction
                        CompletableFuture<Void> future = replayTransaction(transaction);

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

    private CompletableFuture<Void> replayTransaction(LedgerEntrySerializer.TransactionData transaction) {
        // Use the ledger manager to replay the transaction
        return ledgerManager.processTransaction(
            transaction.debitEntry().accountId(),
            transaction.creditEntry().accountId(),
            transaction.debitEntry().amount(),
            transaction.debitEntry().currencyCode(),
            transaction.debitEntry().operationDate(),
            transaction.debitEntry().idempotencyKey()
        ).thenApply(offset -> null);
    }

    private void validateSystemConsistency() {
        logger.info("Validating system consistency after recovery");

        // This would include checks like:
        // - Verify all partitions are healthy
        // - Check that database and log offsets are consistent
        // - Validate account balance consistency

        Map<Integer, EnhancedLedgerPartition> partitions = ledgerManager.getPartitions();

        for (Map.Entry<Integer, EnhancedLedgerPartition> entry : partitions.entrySet()) {
            int partitionId = entry.getKey();
            EnhancedLedgerPartition partition = entry.getValue();

            if (!partition.isHealthy()) {
                throw new RuntimeException("Partition " + partitionId + " is not healthy after recovery");
            }
        }

        logger.info("System consistency validation passed");
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

// Scheduled checkpoint creation service
@Component
public class CheckpointService {
    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    @Autowired
    private HybridLedgerManager ledgerManager;

    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void createPeriodicCheckpoints() {
        logger.debug("Starting periodic checkpoint creation");

        try {
            Map<Integer, EnhancedLedgerPartition> partitions = ledgerManager.getPartitions();

            for (EnhancedLedgerPartition partition : partitions.values()) {
                partition.createCheckpoint();
            }

            logger.info("Successfully created checkpoints for {} partitions", partitions.size());

        } catch (Exception e) {
            logger.error("Failed to create periodic checkpoints", e);
        }
    }

    public void createManualCheckpoint() {
        logger.info("Creating manual checkpoint");

        try {
            Map<Integer, EnhancedLedgerPartition> partitions = ledgerManager.getPartitions();

            for (EnhancedLedgerPartition partition : partitions.values()) {
                partition.createCheckpoint();
            }

            logger.info("Manual checkpoint creation completed for {} partitions", partitions.size());

        } catch (Exception e) {
            logger.error("Manual checkpoint creation failed", e);
            throw new RuntimeException("Checkpoint creation failed", e);
        }
    }
}

// Extended HybridLedgerManager with partition access for recovery
public class HybridLedgerManager implements AutoCloseable {
    // ... existing code ...

    // Add method to expose partitions for recovery service
    public Map<Integer, EnhancedLedgerPartition> getPartitions() {
        return Collections.unmodifiableMap(partitions);
    }

    // Recovery-aware initialization
    @PostConstruct
    public void initialize() throws IOException {
        logger.info("Initializing hybrid ledger manager with recovery support");

        // Initialize partitions first
        Path logDir = Paths.get(logDirectory);
        Files.createDirectories(logDir);

        for (int i = 0; i < numPartitions; i++) {
            EnhancedLedgerPartition partition = new EnhancedLedgerPartition(
                i, logDir, accountCache, lockManager, circuitBreaker, rateLimiter
            );
            partitions.put(i, partition);
        }

        // Start disruptor
        disruptor.start();

        // Start batch processor
        batchProcessor.scheduleAtFixedRate(this::processDatabaseBatch, 50, 50, TimeUnit.MILLISECONDS);

        logger.info("Hybrid ledger manager initialized with {} partitions", numPartitions);

        // Note: Recovery will be performed by LedgerRecoveryService via @EventListener
    }
}

// Enhanced LedgerRepository interface with recovery methods
@Repository
public interface LedgerRepository {
    void insertBatch(List<LedgerEntry> entries, List<WalEntry> walEntries);
    long calculateBalance(UUID accountId, LocalDate operationDate);
    Optional<BalanceSnapshot> getLatestSnapshot(UUID accountId, LocalDate operationDate);
    List<LedgerEntry> getEntriesAfterOrdinal(UUID accountId, long afterOrdinal, LocalDate operationDate);

    // Recovery-specific methods
    List<WalEntry> getPendingWalEntries(int limit);
    void updateWalEntryStatus(UUID walEntryId, WalEntry.WalStatus status, String errorMessage);

    // Access to JdbcTemplate for recovery service
    JdbcTemplate getJdbcTemplate();
}

// Complete PostgreSQL implementation
@Repository
public class PostgreSQLLedgerRepository implements LedgerRepository {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLLedgerRepository.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedJdbcTemplate;

    @Override
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

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

    @Override
    public void updateWalEntryStatus(UUID walEntryId, WalEntry.WalStatus status, String errorMessage) {
        String sql = """
            UPDATE ledger.wal_entries 
            SET status = ?::ledger.wal_status, processed_at = NOW(), error_message = ?
            WHERE id = ?
            """;

        jdbcTemplate.update(sql, status.name(), errorMessage, walEntryId);
    }
}