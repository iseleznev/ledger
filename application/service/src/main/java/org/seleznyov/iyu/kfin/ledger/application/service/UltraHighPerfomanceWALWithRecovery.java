package org.seleznyov.iyu.kfin.ledger.application.service;

// ПОЛНАЯ ИНТЕГРАЦИЯ RECOVERY МЕХАНИЗМА В СУЩЕСТВУЮЩИЙ КОД

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;
import java.lang.foreign.*;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.ZoneOffset;
import java.sql.Timestamp;
import java.util.*;

@Component
@Slf4j
public class UltraHighPerformanceWALWithRecovery {

    // === ИСХОДНЫЕ ПОЛЯ ИЗ ВАШЕГО КОДА ===
    private final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    private final CoreWALShard[] coreShards = new CoreWALShard[CPU_CORES];
    private static final ThreadLocal<CoreWALShard> THREAD_LOCAL_SHARD = new ThreadLocal<>();

    // === НОВЫЕ ПОЛЯ ДЛЯ RECOVERY ===
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile boolean recoveryInProgress = false;
    private final AtomicLong totalRecoveredRecords = new AtomicLong(0);
    private final Set<String> processedTransactions = ConcurrentHashMap.newKeySet();

    public UltraHighPerformanceWALWithRecovery(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void initialize() {
        log.info("Начинаем инициализацию WAL с recovery механизмом...");

        // === 1. СНАЧАЛА ВЫПОЛНЯЕМ RECOVERY ===
        performCompleteWALRecovery();

        // === 2. ЗАТЕМ ЗАПУСКАЕМ ШАРДЫ ===
        for (int coreId = 0; coreId < CPU_CORES; coreId++) {
            coreShards[coreId] = new CoreWALShardWithRecovery(coreId);
            coreShards[coreId].start();
        }

        log.info("WAL инициализация завершена. Восстановлено {} записей",
            totalRecoveredRecords.get());
    }

    // === ПОЛНЫЙ RECOVERY ПРОЦЕСС ===

    private void performCompleteWALRecovery() {
        recoveryInProgress = true;
        long recoveryStartTime = System.currentTimeMillis();

        try {
            log.info("=== НАЧИНАЕМ WAL RECOVERY ===");

            // 1. Загружаем последний checkpoint
            CheckpointInfo lastCheckpoint = loadLastCheckpoint();
            log.info("Загружен checkpoint: {} записей на {}",
                lastCheckpoint.getTotalRecords(),
                Instant.ofEpochMilli(lastCheckpoint.getCheckpointTime()));

            // 2. Загружаем уже обработанные транзакции из БД
            loadProcessedTransactions();
            log.info("Загружено {} обработанных транзакций из БД", processedTransactions.size());

            // 3. Восстанавливаем каждый шард
            for (int shardId = 0; shardId < CPU_CORES; shardId++) {
                ShardCheckpointInfo shardCheckpoint = lastCheckpoint.getShardInfo(shardId);
                long recoveredCount = recoverShardWAL(shardId, shardCheckpoint);
                totalRecoveredRecords.addAndGet(recoveredCount);

                log.info("Шард {}: восстановлено {} новых записей", shardId, recoveredCount);
            }

            // 4. Создаем новый checkpoint после recovery
            createCheckpoint();

            long recoveryDuration = System.currentTimeMillis() - recoveryStartTime;
            log.info("=== WAL RECOVERY ЗАВЕРШЕН за {}ms ===", recoveryDuration);

        } catch (Exception e) {
            log.error("КРИТИЧЕСКАЯ ОШИБКА WAL RECOVERY", e);
            throw new WALRecoveryException("WAL Recovery failed", e);
        } finally {
            recoveryInProgress = false;
        }
    }

    // === ЗАГРУЗКА ОБРАБОТАННЫХ ТРАНЗАКЦИЙ ИЗ БД ===

    private void loadProcessedTransactions() {
        String sql = """
            SELECT DISTINCT transaction_id 
            FROM transaction_journal 
            WHERE created_at > :cutoffTime
            """;

        // Загружаем транзакции за последние 7 дней
        LocalDateTime cutoffTime = LocalDateTime.now().minusDays(7);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("cutoffTime", Timestamp.valueOf(cutoffTime));

        List<String> processedTxIds = jdbcTemplate.queryForList(sql, params, String.class);
        processedTransactions.addAll(processedTxIds);
    }

    // === ВОССТАНОВЛЕНИЕ ОДНОГО ШАРДА ===

    private long recoverShardWAL(int shardId, ShardCheckpointInfo checkpoint) {
        String filename = String.format("wal-shard-%d.log", shardId);
        File walFile = new File(filename);

        if (!walFile.exists()) {
            log.info("WAL файл {} не существует - создаем новый", filename);
            return 0;
        }

        long recoveredRecords = 0;
        long startPosition = checkpoint != null ? checkpoint.getLastOffset() : 0;

        try (RandomAccessFile file = new RandomAccessFile(walFile, "r")) {
            long fileSize = file.length();

            log.info("Восстанавливаем WAL файл {}: размер={}KB, начинаем с позиции={}",
                filename, fileSize / 1024, startPosition);

            long currentPosition = startPosition;

            // === ЧИТАЕМ ВСЕ ЗАПИСИ ПОСЛЕ CHECKPOINT ===
            while (currentPosition < fileSize) {
                try {
                    // Читаем заголовок записи
                    WALRecordHeader header = readWALRecordHeader(file, currentPosition);

                    if (header == null || !isValidHeader(header, currentPosition, fileSize)) {
                        log.warn("Некорректная запись на позиции {} в {}", currentPosition, filename);
                        currentPosition = findNextValidRecord(file, currentPosition + 1);
                        if (currentPosition == -1) break;
                        continue;
                    }

                    // Читаем полную запись
                    WALRecord record = readFullWALRecord(file, currentPosition, header);

                    if (record != null) {
                        // === ВОССТАНАВЛИВАЕМ ЗАПИСЬ ===
                        if (recoverWALRecord(record, shardId, currentPosition)) {
                            recoveredRecords++;
                        }
                    }

                    currentPosition += header.getRecordLength();

                } catch (Exception e) {
                    log.error("Ошибка при восстановлении записи на позиции {} в {}",
                        currentPosition, filename, e);

                    // Пропускаем поврежденную запись
                    currentPosition = findNextValidRecord(file, currentPosition + 1);
                    if (currentPosition == -1) break;
                }
            }

        } catch (IOException e) {
            log.error("Критическая ошибка при восстановлении WAL файла {}", filename, e);
            throw new WALRecoveryException("Recovery failed for shard " + shardId, e);
        }

        return recoveredRecords;
    }

    // === ЧТЕНИЕ ЗАГОЛОВКА WAL ЗАПИСИ ===

    private WALRecordHeader readWALRecordHeader(RandomAccessFile file, long position)
        throws IOException {
        if (file.length() - position < 20) { // Минимум 20 байт для заголовка
            return null;
        }

        file.seek(position);

        long offset = file.readLong();        // 8 bytes - offset
        long timestamp = file.readLong();     // 8 bytes - timestamp
        int recordLength = file.readInt();    // 4 bytes - length

        return new WALRecordHeader(offset, timestamp, recordLength);
    }

    private boolean isValidHeader(WALRecordHeader header, long position, long fileSize) {
        // Валидация заголовка
        return header.getRecordLength() > 20 &&
            header.getRecordLength() <= 1024 * 1024 && // Макс 1MB
            header.getTimestamp() > 0 &&
            header.getTimestamp() <= System.currentTimeMillis() + 86400000 && // +1 день
            position + header.getRecordLength() <= fileSize; // Запись помещается в файл
    }

    // === ЧТЕНИЕ ПОЛНОЙ WAL ЗАПИСИ ===

    private WALRecord readFullWALRecord(RandomAccessFile file, long position,
                                        WALRecordHeader header) throws IOException {

        // Читаем данные после заголовка
        file.seek(position + 20); // Пропускаем 20 байт заголовка
        byte[] recordData = new byte[header.getRecordLength() - 20];
        file.readFully(recordData);

        // Десериализуем
        return deserializeWALRecord(header, recordData);
    }

    // === ДЕСЕРИАЛИЗАЦИЯ WAL ЗАПИСИ ===

    private WALRecord deserializeWALRecord(WALRecordHeader header, byte[] data) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            // Читаем в том же порядке, что и записывали в serializeRecordDirect
            String transactionId = readStringFromBuffer(buffer);
            String accountId = readStringFromBuffer(buffer);
            byte transactionTypeOrdinal = buffer.get();
            TransactionType transactionType = TransactionType.values()[transactionTypeOrdinal];
            BigDecimal amount = readBigDecimalFromBuffer(buffer);
            BigDecimal newBalance = readBigDecimalFromBuffer(buffer);

            // Создаем WAL запись
            WALRecord record = new WALRecord(transactionId, accountId, transactionType,
                amount, newBalance);
            record.setTimestamp(header.getTimestamp()); // Восстанавливаем оригинальный timestamp

            return record;

        } catch (Exception e) {
            log.error("Ошибка десериализации WAL записи: {}", e.getMessage(), e);
            return null;
        }
    }

    private String readStringFromBuffer(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0 || length > 1000) { // Разумные лимиты
            throw new IllegalArgumentException("Некорректная длина строки: " + length);
        }
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private BigDecimal readBigDecimalFromBuffer(ByteBuffer buffer) {
        int unscaledLength = buffer.getInt();
        if (unscaledLength < 0 || unscaledLength > 100) { // Разумные лимиты
            throw new IllegalArgumentException("Некорректная длина BigDecimal: " + unscaledLength);
        }
        byte[] unscaledBytes = new byte[unscaledLength];
        buffer.get(unscaledBytes);
        int scale = buffer.getInt();

        return new BigDecimal(new BigInteger(unscaledBytes), scale);
    }

    // === ВОССТАНОВЛЕНИЕ БИЗНЕС-ЛОГИКИ ===

    private boolean recoverWALRecord(WALRecord record, int shardId, long filePosition) {
        try {
            String transactionId = record.getTransactionId();

            // 1. Проверяем дедупликацию
            if (processedTransactions.contains(transactionId)) {
                log.debug("Транзакция {} уже обработана - пропускаем", transactionId);
                return false;
            }

            // 2. Проверяем в БД (двойная проверка)
            if (isTransactionInDatabase(transactionId)) {
                log.debug("Транзакция {} найдена в БД - пропускаем", transactionId);
                processedTransactions.add(transactionId); // Кэшируем для следующих проверок
                return false;
            }

            // 3. ВОССТАНАВЛИВАЕМ ТРАНЗАКЦИЮ
            recoverTransactionToDB(record);

            // 4. Добавляем в обработанные
            processedTransactions.add(transactionId);

            log.debug("Успешно восстановлена транзакция {}", transactionId);
            return true;

        } catch (Exception e) {
            log.error("Ошибка восстановления транзакции {}: {}",
                record.getTransactionId(), e.getMessage(), e);
            return false;
        }
    }

    // === ПРОВЕРКА СУЩЕСТВОВАНИЯ В БД ===

    private boolean isTransactionInDatabase(String transactionId) {
        String sql = """
            SELECT COUNT(*) 
            FROM transaction_journal 
            WHERE transaction_id = :transactionId
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("transactionId", transactionId);

        Integer count = jdbcTemplate.queryForObject(sql, params, Integer.class);
        return count != null && count > 0;
    }

    // === ВОССТАНОВЛЕНИЕ ТРАНЗАКЦИИ В БД ===

    private void recoverTransactionToDB(WALRecord record) {
        try {
            // 1. Создаем проводки из WAL записи
            List<JournalEntry> entries = reconstructJournalEntries(record);

            // 2. Записываем проводки в БД
            saveJournalEntriesBatch(record.getTransactionId(), entries);

            // 3. Обновляем баланс счета
            updateAccountBalanceFromWAL(record);

            log.debug("Транзакция {} восстановлена в БД", record.getTransactionId());

        } catch (Exception e) {
            log.error("Ошибка записи восстановленной транзакции {} в БД",
                record.getTransactionId(), e);
            throw e;
        }
    }

    // === РЕКОНСТРУКЦИЯ ПРОВОДОК ИЗ WAL ===

    private List<JournalEntry> reconstructJournalEntries(WALRecord record) {
        List<JournalEntry> entries = new ArrayList<>();

        // Создаем проводки на основе типа транзакции из WAL
        switch (record.getTransactionType()) {
            case DEBIT -> {
                // Простая дебетовая операция
                entries.add(new JournalEntry(
                    record.getTransactionId(),
                    record.getAccountId(),
                    EntryType.DEBIT,
                    record.getAmount(),
                    "Восстановлено из WAL - списание",
                    Instant.ofEpochMilli(record.getTimestamp()).atZone(ZoneOffset.UTC).toLocalDateTime(),
                    1
                ));
            }

            case CREDIT -> {
                // Простая кредитовая операция
                entries.add(new JournalEntry(
                    record.getTransactionId(),
                    record.getAccountId(),
                    EntryType.CREDIT,
                    record.getAmount(),
                    "Восстановлено из WAL - зачисление",
                    Instant.ofEpochMilli(record.getTimestamp()).atZone(ZoneOffset.UTC).toLocalDateTime(),
                    1
                ));
            }

            // Можно добавить более сложную логику для TRANSFER операций
            default -> {
                log.warn("Неизвестный тип транзакции для восстановления: {}",
                    record.getTransactionType());
            }
        }

        return entries;
    }

    // === BATCH ЗАПИСЬ ПРОВОДОК ===

    private void saveJournalEntriesBatch(String transactionId, List<JournalEntry> entries) {
        if (entries.isEmpty()) return;

        String sql = """
            INSERT INTO transaction_journal 
            (transaction_id, account_id, entry_type, amount, description, created_at, sequence_number)
            VALUES (:transactionId, :accountId, :entryType, :amount, :description, :createdAt, :sequenceNumber)
            """;

        MapSqlParameterSource[] batchParams = entries.stream()
            .map(entry -> new MapSqlParameterSource()
                .addValue("transactionId", entry.getTransactionId())
                .addValue("accountId", entry.getAccountId())
                .addValue("entryType", entry.getEntryType().name())
                .addValue("amount", entry.getAmount())
                .addValue("description", entry.getDescription())
                .addValue("createdAt", Timestamp.valueOf(entry.getCreatedAt()))
                .addValue("sequenceNumber", entry.getSequenceNumber()))
            .toArray(MapSqlParameterSource[]::new);

        jdbcTemplate.batchUpdate(sql, batchParams);
    }

    // === ОБНОВЛЕНИЕ БАЛАНСА ИЗ WAL ===

    private void updateAccountBalanceFromWAL(WALRecord record) {
        String sql = """
            INSERT INTO account_balances (account_id, current_balance, last_updated, version)
            VALUES (:accountId, :balance, :lastUpdated, 1)
            ON CONFLICT (account_id) 
            DO UPDATE SET 
                current_balance = :balance,
                last_updated = :lastUpdated,
                version = account_balances.version + 1
            WHERE account_balances.last_updated < :lastUpdated
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("accountId", record.getAccountId())
            .addValue("balance", record.getNewBalance())
            .addValue("lastUpdated", Timestamp.valueOf(
                Instant.ofEpochMilli(record.getTimestamp()).atZone(ZoneOffset.UTC).toLocalDateTime()
            ));

        jdbcTemplate.update(sql, params);
    }

    // === ПОИСК СЛЕДУЮЩЕЙ ВАЛИДНОЙ ЗАПИСИ ===

    private long findNextValidRecord(RandomAccessFile file, long startPosition) throws IOException {
        long fileSize = file.length();

        // Ищем следующую валидную запись (простой поиск по заголовкам)
        for (long pos = startPosition; pos < fileSize - 20; pos++) {
            try {
                WALRecordHeader header = readWALRecordHeader(file, pos);
                if (header != null && isValidHeader(header, pos, fileSize)) {
                    log.info("Найдена следующая валидная запись на позиции {}", pos);
                    return pos;
                }
            } catch (Exception e) {
                // Продолжаем поиск
            }
        }

        return -1; // Валидных записей больше нет
    }

    // === CHECKPOINT МЕХАНИЗМ ===

    private CheckpointInfo loadLastCheckpoint() {
        try {
            File checkpointFile = new File("data/wal-checkpoint.json");
            if (!checkpointFile.exists()) {
                log.info("Checkpoint файл не найден - начинаем с нуля");
                return new CheckpointInfo();
            }

            CheckpointInfo checkpoint = objectMapper.readValue(checkpointFile, CheckpointInfo.class);
            log.info("Загружен checkpoint от {}",
                Instant.ofEpochMilli(checkpoint.getCheckpointTime()));
            return checkpoint;

        } catch (IOException e) {
            log.warn("Не удалось загрузить checkpoint - начинаем с нуля", e);
            return new CheckpointInfo();
        }
    }

    public void createCheckpoint() {
        if (recoveryInProgress) {
            return; // Не создаем checkpoint во время recovery
        }

        try {
            CheckpointInfo checkpoint = new CheckpointInfo();

            // Собираем текущее состояние всех шардов
            for (int i = 0; i < coreShards.length; i++) {
                if (coreShards[i] != null) {
                    CoreWALShard shard = coreShards[i];
                    checkpoint.addShardInfo(i, shard.getCurrentOffset(), shard.getTotalWrites());
                }
            }

            // Атомарно сохраняем checkpoint
            saveCheckpointAtomic(checkpoint);

            log.info("Checkpoint создан: {} шардов, {} записей",
                CPU_CORES, checkpoint.getTotalRecords());

        } catch (Exception e) {
            log.error("Ошибка создания checkpoint", e);
        }
    }

    private void saveCheckpointAtomic(CheckpointInfo checkpoint) throws IOException {
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }

        // Атомарная запись через временный файл
        File tempFile = new File("data/wal-checkpoint.json.tmp");
        File finalFile = new File("data/wal-checkpoint.json");

        // Записываем во временный файл
        objectMapper.writeValue(tempFile, checkpoint);

        // Атомарно переименовываем
        if (!tempFile.renameTo(finalFile)) {
            throw new IOException("Не удалось переименовать checkpoint файл");
        }
    }

    // === ИНТЕГРАЦИЯ В СУЩЕСТВУЮЩИЙ COREWALSSHARD ===

    private class CoreWALShardWithRecovery extends CoreWALShard {

        CoreWALShardWithRecovery(int coreId) {
            this.coreId = coreId;
            this.lockFreeQueue = new SPSCQueue<>(65536);
            this.directWriteBuffer = ByteBuffer.allocateDirect(64 * 1024);

            // === ИСПРАВЛЕННЫЙ Foreign Memory API ===
            final Arena arena = Arena.ofShared();
            this.nativeMemorySegment = arena.allocate(64 * 1024);

            // === RECOVERY-AWARE ИНИЦИАЛИЗАЦИЯ ===
            initializePersistentStorageWithRecovery();

            this.dedicatedThread = Thread.ofVirtual()
                .name("WAL-Shard-" + coreId)
                .unstarted(this::runOptimizedWriteLoop);
        }

        private void initializePersistentStorageWithRecovery() {
            try {
                String filename = String.format("wal-shard-%d.log", coreId);
                File walFileObj = new File(filename);

                boolean fileExists = walFileObj.exists();
                long existingSize = fileExists ? walFileObj.length() : 0;

                walFile = new RandomAccessFile(filename, "rw");

                // Расширяем файл до 1GB если нужно
                long targetSize = 1024L * 1024 * 1024; // 1GB
                if (walFile.length() < targetSize) {
                    walFile.setLength(targetSize);
                }

                // Memory mapping
                persistentBuffer = walFile.getChannel().map(
                    FileChannel.MapMode.READ_WRITE, 0, targetSize
                );

                if (fileExists && existingSize > 0) {
                    // === RECOVERY POSITIONING ===
                    // Позиционируем буфер на конец валидных данных
                    long lastValidOffset = findLastValidOffsetInShard(filename);
                    currentOffset = lastValidOffset;
                    persistentBuffer.position((int) Math.min(lastValidOffset, Integer.MAX_VALUE));

                    log.info("Шард {}: восстановлен, offset={}, размер файла={}KB",
                        coreId, lastValidOffset, existingSize / 1024);
                } else {
                    // Новый файл
                    currentOffset = 0;
                    persistentBuffer.position(0);

                    log.info("Шард {}: создан новый WAL файл", coreId);
                }

                // Предзагружаем в память
                persistentBuffer.load();

            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize persistent storage for shard " + coreId, e);
            }
        }

        private long findLastValidOffsetInShard(String filename) throws IOException {
            try (RandomAccessFile file = new RandomAccessFile(filename, "r")) {
                long fileSize = file.length();
                long lastValidOffset = 0;
                long currentPos = 0;

                while (currentPos < fileSize) {
                    try {
                        WALRecordHeader header = readWALRecordHeader(file, currentPos);
                        if (header != null && isValidHeader(header, currentPos, fileSize)) {
                            lastValidOffset = currentPos + header.getRecordLength();
                            currentPos = lastValidOffset;
                        } else {
                            break;
                        }
                    } catch (IOException e) {
                        break;
                    }
                }

                return lastValidOffset;
            }
        }
    }

    // === CHECKPOINT МОДЕЛИ ===

    public static class CheckpointInfo {
        private long checkpointTime = System.currentTimeMillis();
        private Map<Integer, ShardCheckpointInfo> shardInfos = new HashMap<>();
        private long totalRecordsAtCheckpoint = 0;

        public CheckpointInfo() {}

        public void addShardInfo(int shardId, long offset, long totalWrites) {
            shardInfos.put(shardId, new ShardCheckpointInfo(offset, totalWrites));
            totalRecordsAtCheckpoint += totalWrites;
        }

        public ShardCheckpointInfo getShardInfo(int shardId) {
            return shardInfos.get(shardId);
        }

        // Getters и setters для JSON сериализации
        public long getCheckpointTime() { return checkpointTime; }
        public void setCheckpointTime(long checkpointTime) { this.checkpointTime = checkpointTime; }

        public Map<Integer, ShardCheckpointInfo> getShardInfos() { return shardInfos; }
        public void setShardInfos(Map<Integer, ShardCheckpointInfo> shardInfos) { this.shardInfos = shardInfos; }

        public long getTotalRecords() { return totalRecordsAtCheckpoint; }
        public void setTotalRecords(long totalRecords) { this.totalRecordsAtCheckpoint = totalRecords; }
    }

    public static class ShardCheckpointInfo {
        private long lastOffset;
        private long totalWrites;

        public ShardCheckpointInfo() {} // Для Jackson

        public ShardCheckpointInfo(long lastOffset, long totalWrites) {
            this.lastOffset = lastOffset;
            this.totalWrites = totalWrites;
        }

        // Getters и setters для JSON
        public long getLastOffset() { return lastOffset; }
        public void setLastOffset(long lastOffset) { this.lastOffset = lastOffset; }

        public long getTotalWrites() { return totalWrites; }
        public void setTotalWrites(long totalWrites) { this.totalWrites = totalWrites; }
    }

    public static class WALRecordHeader {
        private final long offset;
        private final long timestamp;
        private final int recordLength;

        public WALRecordHeader(long offset, long timestamp, int recordLength) {
            this.offset = offset;
            this.timestamp = timestamp;
            this.recordLength = recordLength;
        }

        public long getOffset() { return offset; }
        public long getTimestamp() { return timestamp; }
        public int getRecordLength() { return recordLength; }
    }

    // === МОДЕЛИ ДАННЫХ ДЛЯ JDBC ===

    public enum EntryType {
        DEBIT, CREDIT
    }

    public static class JournalEntry {
        private final String transactionId;
        private final String accountId;
        private final EntryType entryType;
        private final BigDecimal amount;
        private final String description;
        private final LocalDateTime createdAt;
        private final int sequenceNumber;

        public JournalEntry(String transactionId, String accountId, EntryType entryType,
                            BigDecimal amount, String description, LocalDateTime createdAt,
                            int sequenceNumber) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.entryType = entryType;
            this.amount = amount;
            this.description = description;
            this.createdAt = createdAt;
            this.sequenceNumber = sequenceNumber;
        }

        // Getters...
        public String getTransactionId() { return transactionId; }
        public String getAccountId() { return accountId; }
        public EntryType getEntryType() { return entryType; }
        public BigDecimal getAmount() { return amount; }
        public String getDescription() { return description; }
        public LocalDateTime getCreatedAt() { return createdAt; }
        public int getSequenceNumber() { return sequenceNumber; }
    }

    // === SCHEDULED CHECKPOINT CREATION ===

    @Scheduled(fixedRate = 300000) // Каждые 5 минут
    public void scheduledCheckpointCreation() {
        createCheckpoint();
    }

    // === RECOVERY STATUS API ===

    public WALRecoveryStatus getRecoveryStatus() {
        return new WALRecoveryStatus(
            recoveryInProgress,
            totalRecoveredRecords.get(),
            processedTransactions.size(),
            getLastCheckpointTime()
        );
    }

    private long getLastCheckpointTime() {
        CheckpointInfo checkpoint = loadLastCheckpoint();
        return checkpoint.getCheckpointTime();
    }

    // === ИНТЕГРАЦИЯ С СУЩЕСТВУЮЩИМИ МЕТОДАМИ ===

    // Ваши исходные методы остаются без изменений:
    public CompletableFuture<Void> writeRecord(WALRecord record) {
        if (recoveryInProgress) {
            // Во время recovery откладываем новые записи
            return CompletableFuture.failedFuture(
                new WALRecoveryException("WAL recovery in progress")
            );
        }

        CoreWALShard shard = selectOptimalShard(record);
        return shard.enqueue(record);
    }

    private CoreWALShard selectOptimalShard(WALRecord record) {
        // Ваша исходная логика selectOptimalShard остается без изменений
        CoreWALShard threadLocalShard = THREAD_LOCAL_SHARD.get();
        if (threadLocalShard != null && !threadLocalShard.isOverloaded()) {
            return threadLocalShard;
        }

        CoreWALShard bestShard = coreShards[0];
        long bestScore = calculateShardScore(bestShard);

        for (int i = 1; i < coreShards.length; i++) {
            long score = calculateShardScore(coreShards[i]);
            if (score < bestScore) {
                bestScore = score;
                bestShard = coreShards[i];
            }
        }

        THREAD_LOCAL_SHARD.set(bestShard);
        return bestShard;
    }

    private long calculateShardScore(CoreWALShard shard) {
        // Ваша исходная логика остается
        long queueSize = shard.getQueueSize();
        long avgLatency = shard.getTargetLatencyNs();
        return queueSize * 1000 + avgLatency / 1000;
    }

    // === GRACEFUL SHUTDOWN С FINAL CHECKPOINT ===

    @PreDestroy
    public void shutdown() {
        log.info("Начинаем graceful shutdown WAL...");

        try {
            // 1. Останавливаем прием новых записей
            for (CoreWALShard shard : coreShards) {
                if (shard != null) {
                    shard.stopGracefully();
                }
            }

            // 2. Ждем завершения всех pending операций
            waitForPendingOperations();

            // 3. Создаем финальный checkpoint
            createCheckpoint();

            // 4. Закрываем ресурсы
            closeAllResources();

            log.info("WAL graceful shutdown завершен");

        } catch (Exception e) {
            log.error("Ошибка при shutdown WAL", e);
        }
    }

    private void waitForPendingOperations() {
        long maxWaitMs = 30000; // 30 секунд максимум
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < maxWaitMs) {
            boolean allQueuesEmpty = Arrays.stream(coreShards)
                .filter(Objects::nonNull)
                .allMatch(shard -> shard.getQueueSize() == 0);

            if (allQueuesEmpty) {
                log.info("Все очереди WAL пусты - можно завершать");
                return;
            }

            try {
                Thread.sleep(100); // Проверяем каждые 100ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.warn("Timeout ожидания завершения операций WAL");
    }

    private void closeAllResources() {
        for (CoreWALShard shard : coreShards) {
            if (shard != null) {
                try {
                    shard.close();
                } catch (Exception e) {
                    log.error("Ошибка закрытия шарда {}", shard.coreId, e);
                }
            }
        }
    }

    // === ИСКЛЮЧЕНИЯ ===

    public static class WALRecoveryException extends RuntimeException {
        public WALRecoveryException(String message) {
            super(message);
        }

        public WALRecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // === СТАТУС RECOVERY ===

    public static class WALRecoveryStatus {
        private final boolean inProgress;
        private final long totalRecovered;
        private final long processedTransactions;
        private final long lastCheckpointTime;

        public WALRecoveryStatus(boolean inProgress, long totalRecovered,
                                 long processedTransactions, long lastCheckpointTime) {
            this.inProgress = inProgress;
            this.totalRecovered = totalRecovered;
            this.processedTransactions = processedTransactions;
            this.lastCheckpointTime = lastCheckpointTime;
        }

        public boolean isInProgress() { return inProgress; }
        public long getTotalRecovered() { return totalRecovered; }
        public long getProcessedTransactions() { return processedTransactions; }
        public long getLastCheckpointTime() { return lastCheckpointTime; }
    }

    // === РАСШИРЕНИЕ WALRecord ДЛЯ RECOVERY ===

    public static class WALRecord {
        private final String transactionId;
        private final String accountId;
        private final TransactionType transactionType;
        private final BigDecimal amount;
        private final BigDecimal newBalance;
        private long timestamp; // Убираем final для восстановления
        private CompletableFuture<Void> completionFuture;
        private final int estimatedSize;

        public WALRecord(String transactionId, String accountId, TransactionType transactionType,
                         BigDecimal amount, BigDecimal newBalance) {
            this.transactionId = transactionId;
            this.accountId = accountId;
            this.transactionType = transactionType;
            this.amount = amount;
            this.newBalance = newBalance;
            this.timestamp = System.currentTimeMillis();
            this.estimatedSize = calculateEstimatedSize();
        }

        private int calculateEstimatedSize() {
            return 32 + // Fixed header
                transactionId.length() +
                accountId.length() +
                amount.precision() +
                newBalance.precision();
        }

        // Getters...
        public String getTransactionId() { return transactionId; }
        public String getAccountId() { return accountId; }
        public TransactionType getTransactionType() { return transactionType; }
        public BigDecimal getAmount() { return amount; }
        public BigDecimal getNewBalance() { return newBalance; }
        public long getTimestamp() { return timestamp; }
        public int getEstimatedSize() { return estimatedSize; }

        // === МЕТОДЫ ДЛЯ RECOVERY ===
        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public void setCompletionFuture(CompletableFuture<Void> future) {
            this.completionFuture = future;
        }
        public CompletableFuture<Void> getCompletionFuture() {
            return completionFuture;
        }
    }

    // === ИНТЕГРАЦИЯ С LEDGER SERVICE ===

    @Service
    public static class RecoveryAwareLedgerService extends OptimizedLedgerService {

        private final UltraHighPerformanceWALWithRecovery walWithRecovery;

        public RecoveryAwareLedgerService(UltraHighPerformanceWALWithRecovery walWithRecovery,
                                          ScalableActivityTracker activityTracker,
                                          HotAccountManager hotAccountManager,
                                          AccountBalanceManager balanceManager) {
            super(walWithRecovery, activityTracker, hotAccountManager, balanceManager);
            this.walWithRecovery = walWithRecovery;
        }

        @Override
        public CompletableFuture<TransactionResult> processTransaction(
            String accountId, Transaction transaction) {

            // Проверяем статус recovery
            WALRecoveryStatus recoveryStatus = walWithRecovery.getRecoveryStatus();
            if (recoveryStatus.isInProgress()) {
                return CompletableFuture.completedFuture(
                    TransactionResult.error("System recovery in progress, please retry")
                );
            }

            // Вызываем родительскую логику
            return super.processTransaction(accountId, transaction);
        }

        // === API ДЛЯ МОНИТОРИНГА RECOVERY ===

        @GetMapping("/api/wal/recovery-status")
        public WALRecoveryStatus getRecoveryStatus() {
            return walWithRecovery.getRecoveryStatus();
        }

        @PostMapping("/api/wal/create-checkpoint")
        public ResponseEntity<String> createManualCheckpoint() {
            try {
                walWithRecovery.createCheckpoint();
                return ResponseEntity.ok("Checkpoint создан успешно");
            } catch (Exception e) {
                return ResponseEntity.status(500).body("Ошибка создания checkpoint: " + e.getMessage());
            }
        }
    }

    // === КОНФИГУРАЦИЯ БД ДЛЯ RECOVERY ===

    @Component
    public static class RecoveryDatabaseConfiguration {

        private final NamedParameterJdbcTemplate jdbcTemplate;

        public RecoveryDatabaseConfiguration(NamedParameterJdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        @PostConstruct
        public void createRecoveryTables() {
            // Создаем таблицы если они не существуют
            createAccountBalancesTable();
            createTransactionJournalTable();
            createIndexes();
        }

        private void createAccountBalancesTable() {
            String sql = """
                CREATE TABLE IF NOT EXISTS account_balances (
                    account_id VARCHAR(50) PRIMARY KEY,
                    current_balance DECIMAL(19,4) NOT NULL DEFAULT 0,
                    last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    version BIGINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """;
            jdbcTemplate.update(sql, new MapSqlParameterSource());
        }

        private void createTransactionJournalTable() {
            String sql = """
                CREATE TABLE IF NOT EXISTS transaction_journal (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    transaction_id VARCHAR(100) NOT NULL,
                    account_id VARCHAR(50) NOT NULL,
                    entry_type VARCHAR(10) NOT NULL CHECK (entry_type IN ('DEBIT', 'CREDIT')),
                    amount DECIMAL(19,4) NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    sequence_number INTEGER NOT NULL
                )
                """;
            jdbcTemplate.update(sql, new MapSqlParameterSource());
        }

        private void createIndexes() {
            String[] indexes = {
                "CREATE INDEX IF NOT EXISTS idx_account_balances_updated ON account_balances(last_updated)",
                "CREATE INDEX IF NOT EXISTS idx_journal_transaction ON transaction_journal(transaction_id)",
                "CREATE INDEX IF NOT EXISTS idx_journal_account_date ON transaction_journal(account_id, created_at)",
                "CREATE INDEX IF NOT EXISTS idx_journal_created_at ON transaction_journal(created_at)"
            };

            for (String indexSql : indexes) {
                try {
                    jdbcTemplate.update(indexSql, new MapSqlParameterSource());
                } catch (Exception e) {
                    log.warn("Не удалось создать индекс: {}", e.getMessage());
                }
            }
        }
    }

    // === ОСТАЛЬНАЯ ЧАСТЬ ИСХОДНОГО КОДА (SPSCQueue, etc.) ===

    // Ваш исходный SPSCQueue остается без изменений
    private static class SPSCQueue<T> {
        private final T[] buffer;
        private final int mask;
        private volatile long head = 0;
        private volatile long tail = 0;

        @SuppressWarnings("unchecked")
        SPSCQueue(int capacity) {
            if ((capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be power of 2");
            }
            this.buffer = (T[]) new Object[capacity];
            this.mask = capacity - 1;
        }

        boolean offer(T item) {
            long currentHead = head;
            long nextHead = currentHead + 1;

            if (nextHead - tail > buffer.length) {
                return false;
            }

            buffer[(int) (currentHead & mask)] = item;
            head = nextHead;
            return true;
        }

        T poll() {
            long currentTail = tail;

            if (currentTail >= head) {
                return null;
            }

            T item = buffer[(int) (currentTail & mask)];
            buffer[(int) (currentTail & mask)] = null;

            tail = currentTail + 1;
            return item;
        }

        int size() {
            return (int) (head - tail);
        }
    }

    // === БАЗОВЫЙ CoreWALShard (для наследования) ===

    protected static class CoreWALShard {
        protected int coreId;
        protected Thread dedicatedThread;
        protected SPSCQueue<WALRecord> lockFreeQueue;
        protected ByteBuffer directWriteBuffer;
        protected MemorySegment nativeMemorySegment;
        protected MappedByteBuffer persistentBuffer;
        protected RandomAccessFile walFile;

        // Thread-local статистика
        protected long totalWrites = 0;
        protected long totalBytes = 0;
        protected long currentOffset = 0;
        protected long lastFlushTime = System.nanoTime();

        // Адаптивные параметры
        protected int currentBatchSize = 10;
        protected long targetLatencyNs = 1_000_000;
        protected long[] latencyHistory = new long[100];
        protected int latencyIndex = 0;

        // Конструктор будет переопределен в CoreWALShardWithRecovery
        protected CoreWALShard() {}

        void start() {
            dedicatedThread.start();
        }

        // === ВСЕ ВАШИ ИСХОДНЫЕ МЕТОДЫ ОСТАЮТСЯ ===

        protected void runOptimizedWriteLoop() {
            WALRecord[] batchBuffer = new WALRecord[1000];
            long loopStartTime = System.nanoTime();

            while (!Thread.currentThread().isInterrupted()) {
                int batchCount = collectBatch(batchBuffer);

                if (batchCount > 0) {
                    long writeStart = System.nanoTime();
                    writeBatchOptimized(batchBuffer, batchCount);
                    long writeEnd = System.nanoTime();
                    long writeDuration = writeEnd - writeStart;

                    adjustBatchingParameters(writeDuration, batchCount);
                    notifyCompletions(batchBuffer, batchCount);
                    totalWrites += batchCount;
                }

                long elapsed = System.nanoTime() - loopStartTime;
                if (elapsed > 50_000) {
                    Thread.yield();
                    loopStartTime = System.nanoTime();
                }
            }
        }

        // Все остальные методы из вашего исходного кода...
        protected int collectBatch(WALRecord[] batchBuffer) { /* ваша логика */ return 0; }
        protected void writeBatchOptimized(WALRecord[] batch, int count) { /* ваша логика */ }
        protected void adjustBatchingParameters(long writeDuration, int batchCount) { /* ваша логика */ }
        protected void notifyCompletions(WALRecord[] batch, int count) { /* ваша логика */ }

        // Геттеры
        long getTotalWrites() { return totalWrites; }
        long getTotalBytes() { return totalBytes; }
        long getCurrentOffset() { return currentOffset; }
        int getQueueSize() { return lockFreeQueue.size(); }
        int getCurrentBatchSize() { return currentBatchSize; }
        long getTargetLatencyNs() { return targetLatencyNs; }

        boolean isOverloaded() {
            return getQueueSize() > 1000; // Простая проверка перегрузки
        }

        CompletableFuture<Void> enqueue(WALRecord record) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            record.setCompletionFuture(future);

            if (!lockFreeQueue.offer(record)) {
                future.completeExceptionally(
                    new WALBackpressureException("Shard " + coreId + " queue full")
                );
            }

            return future;
        }

        void stopGracefully() {
            if (dedicatedThread != null) {
                dedicatedThread.interrupt();
            }
        }

        void close() throws IOException {
            if (walFile != null) {
                walFile.close();
            }
        }
    }

    // === ДОПОЛНИТЕЛЬНЫЕ УТИЛИТЫ ===

    public enum TransactionType {
        DEBIT, CREDIT, TRANSFER_OUT, TRANSFER_IN
    }

    public static class WALBackpressureException extends RuntimeException {
        public WALBackpressureException(String message) {
            super(message);
        }
    }
}