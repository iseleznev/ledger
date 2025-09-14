// ===== ПРАВИЛЬНАЯ АРХИТЕКТУРА RECOVERY - ТОЛЬКО ОПЕРАЦИИ =====

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Component
@Slf4j
public class TransactionRecoveryManager {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final AtomicLong recoveredOperations = new AtomicLong(0);
    private final Set<String> processedTransactions = ConcurrentHashMap.newKeySet();

    public TransactionRecoveryManager(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * ГЛАВНЫЙ МЕТОД RECOVERY: Восстанавливает ТОЛЬКО операции из WAL
     *
     * НЕ восстанавливает балансы - они пересчитываются из операций!
     */
    public void recoverOperationsFromWAL(WALRecord record) {
        String transactionId = record.getTransactionId();

        try {
            // 1. Проверяем дедупликацию
            if (processedTransactions.contains(transactionId)) {
                log.debug("Операция {} уже восстановлена - пропускаем", transactionId);
                return;
            }

            // 2. Проверяем в БД
            if (isOperationInDatabase(transactionId)) {
                log.debug("Операция {} найдена в БД - пропускаем", transactionId);
                processedTransactions.add(transactionId);
                return;
            }

            // 3. ВОССТАНАВЛИВАЕМ ОПЕРАЦИЮ (не баланс!)
            recoverSingleOperation(record);

            // 4. Добавляем в обработанные
            processedTransactions.add(transactionId);
            recoveredOperations.incrementAndGet();

            log.debug("Восстановлена операция {}: {} {} на сумму {}",
                transactionId,
                record.getTransactionType(),
                record.getAccountId(),
                record.getAmount());

        } catch (Exception e) {
            log.error("Ошибка восстановления операции {}: {}",
                transactionId, e.getMessage(), e);
        }
    }

    /**
     * ВОССТАНОВЛЕНИЕ ОДНОЙ ОПЕРАЦИИ
     */
    private void recoverSingleOperation(WALRecord record) {
        // Создаем проводку из WAL записи
        JournalEntry entry = createJournalEntryFromWAL(record);

        // Записываем в БД
        saveJournalEntry(entry);

        log.debug("Создана проводка: {} {} {} на сумму {}",
            entry.getTransactionId(),
            entry.getEntryType(),
            entry.getAccountId(),
            entry.getAmount());
    }

    /**
     * СОЗДАНИЕ ПРОВОДКИ ИЗ WAL ЗАПИСИ
     */
    private JournalEntry createJournalEntryFromWAL(WALRecord record) {
        // Определяем тип проводки из WAL
        EntryType entryType = switch (record.getTransactionType()) {
            case DEBIT, TRANSFER_OUT -> EntryType.DEBIT;
            case CREDIT, TRANSFER_IN -> EntryType.CREDIT;
        };

        return new JournalEntry(
            record.getTransactionId(),
            record.getAccountId(),
            entryType,
            record.getAmount(),
            "Восстановлено из WAL", // description
            Instant.ofEpochMilli(record.getTimestamp())
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime(),
            1 // sequence_number
        );
    }

    /**
     * ЗАПИСЬ ПРОВОДКИ В БД
     */
    private void saveJournalEntry(JournalEntry entry) {
        String sql = """
            INSERT INTO transaction_journal 
            (transaction_id, account_id, entry_type, amount, description, created_at, sequence_number)
            VALUES (:transactionId, :accountId, :entryType, :amount, :description, :createdAt, :sequenceNumber)
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("transactionId", entry.getTransactionId())
            .addValue("accountId", entry.getAccountId())
            .addValue("entryType", entry.getEntryType().name())
            .addValue("amount", entry.getAmount())
            .addValue("description", entry.getDescription())
            .addValue("createdAt", Timestamp.valueOf(entry.getCreatedAt()))
            .addValue("sequenceNumber", entry.getSequenceNumber());

        jdbcTemplate.update(sql, params);
    }

    /**
     * BATCH RECOVERY ОПЕРАЦИЙ
     */
    public void recoverOperationsBatch(List<WALRecord> walRecords) {
        if (walRecords.isEmpty()) return;

        log.info("BATCH RECOVERY: Восстанавливаем {} операций", walRecords.size());

        List<JournalEntry> entries = new ArrayList<>();
        Set<String> batchTransactionIds = new HashSet<>();

        // Фильтруем и создаем проводки
        for (WALRecord record : walRecords) {
            String txId = record.getTransactionId();

            // Пропускаем дубли в батче
            if (batchTransactionIds.contains(txId)) {
                continue;
            }

            // Пропускаем уже обработанные
            if (processedTransactions.contains(txId) || isOperationInDatabase(txId)) {
                processedTransactions.add(txId);
                continue;
            }

            JournalEntry entry = createJournalEntryFromWAL(record);
            entries.add(entry);
            batchTransactionIds.add(txId);
        }

        if (entries.isEmpty()) {
            log.info("BATCH RECOVERY: Все операции уже восстановлены");
            return;
        }

        // Массовая запись в БД
        saveJournalEntriesBatch(entries);

        // Обновляем статистику
        processedTransactions.addAll(batchTransactionIds);
        recoveredOperations.addAndGet(entries.size());

        log.info("BATCH RECOVERY: Восстановлено {} новых операций", entries.size());
    }

    private void saveJournalEntriesBatch(List<JournalEntry> entries) {
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

    /**
     * ПРОВЕРКА ОПЕРАЦИИ В БД
     */
    private boolean isOperationInDatabase(String transactionId) {
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

    /**
     * ЗАГРУЗКА УЖЕ ОБРАБОТАННЫХ ОПЕРАЦИЙ ПРИ СТАРТЕ
     */
    public void loadProcessedOperations() {
        String sql = """
            SELECT DISTINCT transaction_id 
            FROM transaction_journal 
            WHERE created_at > :cutoffTime
            """;

        LocalDateTime cutoffTime = LocalDateTime.now().minusDays(7);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("cutoffTime", Timestamp.valueOf(cutoffTime));

        List<String> processedTxIds = jdbcTemplate.queryForList(sql, params, String.class);
        processedTransactions.addAll(processedTxIds);

        log.info("Загружено {} уже обработанных операций", processedTxIds.size());
    }

    /**
     * СТАТИСТИКА RECOVERY
     */
    public Map<String, Object> getRecoveryStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("recoveredOperations", recoveredOperations.get());
        stats.put("processedTransactions", processedTransactions.size());
        stats.put("databaseOperationCount", getTotalOperationsInDB());
        return stats;
    }

    private long getTotalOperationsInDB() {
        String sql = "SELECT COUNT(*) FROM transaction_journal";
        Long count = jdbcTemplate.queryForObject(sql, new MapSqlParameterSource(), Long.class);
        return count != null ? count : 0;
    }
}

// ===== УПРОЩЕННЫЙ AccountBalanceManager БЕЗ RECOVERY БАЛАНСОВ =====

@Component
@Slf4j
public class SnapshotBasedBalanceManager {

    // Кэши остаются такими же
    private final Map<String, AccountBalance> hotBalances = new ConcurrentHashMap<>();
    private final Map<String, AccountBalance> recentBalances = Collections.synchronizedMap(
        new LinkedHashMap<String, AccountBalance>(50000, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, AccountBalance> eldest) {
                return size() > 40000;
            }
        }
    );

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ChronicleMapAccountStorage chronicleStorage;

    public SnapshotBasedBalanceManager(NamedParameterJdbcTemplate jdbcTemplate,
                                       ChronicleMapAccountStorage chronicleStorage) {
        this.jdbcTemplate = jdbcTemplate;
        this.chronicleStorage = chronicleStorage;
    }

    /**
     * ГЛАВНЫЙ МЕТОД: Получение баланса
     *
     * ПОРЯДОК ПОИСКА:
     * 1. Hot cache
     * 2. Recent cache  
     * 3. Chronicle Map снапшот
     * 4. Пересчет из операций в БД
     */
    public AccountBalance getBalance(String accountId) {
        // 1. Hot cache
        AccountBalance balance = hotBalances.get(accountId);
        if (balance != null) {
            return balance;
        }

        // 2. Recent cache
        balance = recentBalances.get(accountId);
        if (balance != null) {
            return balance;
        }

        // 3. Chronicle Map снапшот
        balance = chronicleStorage.getAccountBalance(accountId);
        if (balance != null) {
            recentBalances.put(accountId, balance);
            return balance;
        }

        // 4. Пересчет из операций в БД
        balance = calculateBalanceFromOperations(accountId);
        recentBalances.put(accountId, balance);

        return balance;
    }

    /**
     * ПЕРЕСЧЕТ БАЛАНСА ИЗ ОПЕРАЦИЙ
     *
     * Используется когда нет снапшота или после recovery
     */
    private AccountBalance calculateBalanceFromOperations(String accountId) {
        String sql = """
            SELECT COALESCE(SUM(
                CASE 
                    WHEN entry_type = 'DEBIT' THEN amount 
                    WHEN entry_type = 'CREDIT' THEN -amount 
                    ELSE 0 
                END
            ), 0) as balance
            FROM transaction_journal 
            WHERE account_id = :accountId
            """;

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("accountId", accountId);

        BigDecimal calculatedBalance = jdbcTemplate.queryForObject(sql, params, BigDecimal.class);

        log.debug("Пересчитан баланс {} = {} из операций в БД", accountId, calculatedBalance);

        return new AccountBalance(accountId, calculatedBalance != null ? calculatedBalance : BigDecimal.ZERO);
    }

    /**
     * ПРИМЕНЕНИЕ ОПЕРАЦИИ (остается без изменений)
     */
    public AccountBalance applyTransaction(String accountId, Transaction transaction) {
        AccountBalance currentBalance = getBalance(accountId);

        if (!canProcessTransaction(currentBalance, transaction)) {
            throw new InsufficientFundsException(
                "Insufficient funds for account " + accountId
            );
        }

        BigDecimal newAmount = currentBalance.getAmount();

        switch (transaction.getType()) {
            case DEBIT -> newAmount = newAmount.subtract(transaction.getAmount());
            case CREDIT -> newAmount = newAmount.add(transaction.getAmount());
            case TRANSFER_OUT -> newAmount = newAmount.subtract(transaction.getAmount());
            case TRANSFER_IN -> newAmount = newAmount.add(transaction.getAmount());
        }

        AccountBalance newBalance = new AccountBalance(accountId, newAmount);

        // Обновляем кэш
        if (isHotAccount(accountId)) {
            hotBalances.put(accountId, newBalance);
        } else {
            recentBalances.put(accountId, newBalance);
        }

        return newBalance;
    }

    /**
     * СОЗДАНИЕ СНАПШОТА БАЛАНСА
     *
     * Периодически сохраняем текущие балансы в Chronicle Map
     */
    @Scheduled(fixedRate = 300000) // Каждые 5 минут
    public void createBalanceSnapshot() {
        log.info("Создаем снапшот балансов...");

        int snapshotCount = 0;

        // Сохраняем hot balances в Chronicle Map
        for (Map.Entry<String, AccountBalance> entry : hotBalances.entrySet()) {
            chronicleStorage.updateAccountBalance(entry.getKey(), entry.getValue());
            snapshotCount++;
        }

        log.info("Снапшот создан: {} балансов сохранено", snapshotCount);
    }

    /**
     * ПРИНУДИТЕЛЬНЫЙ ПЕРЕСЧЕТ ВСЕХ БАЛАНСОВ
     *
     * Используется после recovery для обновления кэшей
     */
    public void recalculateAllBalances() {
        log.info("Принудительный пересчет всех балансов из операций...");

        // Очищаем все кэши
        hotBalances.clear();
        recentBalances.clear();

        // Получаем все уникальные счета из БД
        String sql = """
            SELECT DISTINCT account_id 
            FROM transaction_journal 
            ORDER BY account_id
            """;

        List<String> allAccounts = jdbcTemplate.queryForList(sql, new MapSqlParameterSource(), String.class);

        log.info("Пересчитываем балансы для {} счетов", allAccounts.size());

        // Пересчитываем каждый счет
        for (String accountId : allAccounts) {
            AccountBalance recalculated = calculateBalanceFromOperations(accountId);
            recentBalances.put(accountId, recalculated);
        }

        log.info("Пересчет завершен для {} счетов", allAccounts.size());
    }

    // Остальные utility методы без изменений...
    private boolean canProcessTransaction(AccountBalance balance, Transaction transaction) {
        return switch (transaction.getType()) {
            case DEBIT, TRANSFER_OUT -> balance.getAmount().compareTo(transaction.getAmount()) >= 0;
            case CREDIT, TRANSFER_IN -> true;
        };
    }

    private boolean isHotAccount(String accountId) {
        return hotBalances.containsKey(accountId);
    }
}

// ===== ОБНОВЛЕННЫЙ WAL RECOVERY ПРОЦЕСС =====

@Component
@Slf4j
public class CorrectWALRecoveryService {

    private final TransactionRecoveryManager transactionRecovery;
    private final SnapshotBasedBalanceManager balanceManager;

    public CorrectWALRecoveryService(TransactionRecoveryManager transactionRecovery,
                                     SnapshotBasedBalanceManager balanceManager) {
        this.transactionRecovery = transactionRecovery;
        this.balanceManager = balanceManager;
    }

    /**
     * ПРАВИЛЬНЫЙ RECOVERY ПРОЦЕСС:
     * 1. Загружаем уже обработанные операции
     * 2. Восстанавливаем недостающие операции из WAL
     * 3. Пересчитываем балансы из всех операций
     * 4. Создаем новые снапшоты
     */
    @PostConstruct
    public void performCorrectRecovery() {
        log.info("=== НАЧИНАЕМ ПРАВИЛЬНЫЙ WAL RECOVERY ===");

        try {
            // 1. Загружаем обработанные операции
            transactionRecovery.loadProcessedOperations();

            // 2. Восстанавливаем операции из WAL файлов
            recoverOperationsFromAllShards();

            // 3. Пересчитываем все балансы из операций
            balanceManager.recalculateAllBalances();

            // 4. Создаем актуальные снапшоты
            balanceManager.createBalanceSnapshot();

            log.info("=== RECOVERY ЗАВЕРШЕН УСПЕШНО ===");

        } catch (Exception e) {
            log.error("КРИТИЧЕСКАЯ ОШИБКА RECOVERY", e);
            throw new WALRecoveryException("Recovery failed", e);
        }
    }

    private void recoverOperationsFromAllShards() {
        int totalRecovered = 0;

        for (int shardId = 0; shardId < Runtime.getRuntime().availableProcessors(); shardId++) {
            int recovered = recoverShardOperations(shardId);
            totalRecovered += recovered;
            log.info("Шард {}: восстановлено {} операций", shardId, recovered);
        }

        log.info("Всего восстановлено {} операций из всех шардов", totalRecovered);
    }

    private int recoverShardOperations(int shardId) {
        String filename = String.format("wal-shard-%d.log", shardId);
        File walFile = new File(filename);

        if (!walFile.exists()) {
            return 0;
        }

        List<WALRecord> recoveredRecords = new ArrayList<>();

        try (RandomAccessFile file = new RandomAccessFile(walFile, "r")) {
            long fileSize = file.length();
            long currentPosition = 0;

            while (currentPosition < fileSize) {
                try {
                    WALRecord record = readWALRecord(file, currentPosition);
                    if (record != null) {
                        recoveredRecords.add(record);
                        currentPosition += record.getEstimatedSize();
                    } else {
                        break;
                    }
                } catch (Exception e) {
                    log.warn("Ошибка чтения WAL записи на позиции {} в {}", currentPosition, filename);
                    break;
                }
            }

            // Batch восстановление операций
            transactionRecovery.recoverOperationsBatch(recoveredRecords);

            return recoveredRecords.size();

        } catch (IOException e) {
            log.error("Ошибка чтения WAL файла {}", filename, e);
            return 0;
        }
    }

    private WALRecord readWALRecord(RandomAccessFile file, long position) throws IOException {
        // Реализация чтения WAL записи
        // (используем методы из предыдущих артефактов)
        return null; // Заглушка
    }
}

// ===== МОДЕЛИ ДАННЫХ (БЕЗ ИЗМЕНЕНИЙ) =====

public static class AccountBalance {
    private final String accountId;
    private final BigDecimal amount;
    private final long lastUpdated;

    public AccountBalance(String accountId, BigDecimal amount) {
        this.accountId = accountId;
        this.amount = amount;
        this.lastUpdated = System.currentTimeMillis();
    }

    // Getters...
    public String getAccountId() { return accountId; }
    public BigDecimal getAmount() { return amount; }
    public long getLastUpdated() { return lastUpdated; }
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

public enum EntryType {
    DEBIT, CREDIT
}

public static class WALRecoveryException extends RuntimeException {
    public WALRecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}

public static class InsufficientFundsException extends RuntimeException {
    public InsufficientFundsException(String message) {
        super(message);
    }
}