package org.seleznyov.iyu.kfin.ledger.application.service;

// АСИНХРОННАЯ СИСТЕМА СОЗДАНИЯ СНАПШОТОВ ДЛЯ БАЛАНСОВ СЧЕТОВ

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;

// ===== 1. ОСНОВНОЙ СЕРВИС СНАПШОТОВ =====

@Service
@Transactional
public class AsynchronousSnapshotService {

    private final SnapshotRepository snapshotRepository;
    private final EntryRecordsRepository entryRecordsRepository;
    private final UltraHighPerformanceWAL walService;

    // Virtual Thread Executor для асинхронных снапшотов
    private final ExecutorService snapshotExecutor;

    // Очередь счетов для создания снапшотов
    private final ConcurrentLinkedQueue<SnapshotRequest> snapshotQueue;
    private final AtomicBoolean snapshotProcessorRunning = new AtomicBoolean(false);

    // Статистика создания снапшотов
    private final AtomicLong totalSnapshotsCreated = new AtomicLong(0);
    private final AtomicLong totalSnapshotTime = new AtomicLong(0);
    private final AtomicInteger activeSnapshotTasks = new AtomicInteger(0);

    // Конфигурация
    @Value("${ledger.snapshot.batch-size:100}")
    private int snapshotBatchSize;

    @Value("${ledger.snapshot.max-concurrent:10}")
    private int maxConcurrentSnapshots;

    @Value("${ledger.snapshot.min-operations-threshold:50}")
    private int minOperationsThreshold;

    public AsynchronousSnapshotService(SnapshotRepository snapshotRepository,
                                       EntryRecordsRepository entryRecordsRepository,
                                       UltraHighPerformanceWAL walService) {
        this.snapshotRepository = snapshotRepository;
        this.entryRecordsRepository = entryRecordsRepository;
        this.walService = walService;
        this.snapshotQueue = new ConcurrentLinkedQueue<>();

        // Создаем virtual thread executor для снапшотов
        this.snapshotExecutor = Executors.newVirtualThreadPerTaskExecutor();

        log.info("Asynchronous Snapshot Service initialized with batch size: {}, max concurrent: {}",
            snapshotBatchSize, maxConcurrentSnapshots);
    }

    @PostConstruct
    public void startSnapshotProcessor() {
        // Запускаем фоновый процессор очереди снапшотов
        snapshotExecutor.submit(this::processSnapshotQueue);
        log.info("Snapshot queue processor started");
    }

    /**
     * Запрос на создание снапшота (асинхронный)
     */
    public CompletableFuture<SnapshotResult> requestSnapshot(UUID accountId, LocalDate operationDate) {
        SnapshotRequest request = new SnapshotRequest(
            accountId,
            operationDate,
            System.currentTimeMillis(),
            new CompletableFuture<>()
        );

        snapshotQueue.offer(request);

        log.debug("Snapshot requested for account {} on date {}", accountId, operationDate);

        return request.resultFuture();
    }

    /**
     * Фоновый процессор очереди снапшотов
     */
    private void processSnapshotQueue() {
        log.info("Snapshot queue processor thread started");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                List<SnapshotRequest> batch = collectBatch();

                if (!batch.isEmpty()) {
                    processBatchAsync(batch);
                } else {
                    // Если очередь пустая - ждем немного
                    Thread.sleep(100);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Snapshot queue processor interrupted");
                break;
            } catch (Exception e) {
                log.error("Error in snapshot queue processor", e);
                // Продолжаем работу после ошибки
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Snapshot queue processor stopped");
    }

    private List<SnapshotRequest> collectBatch() {
        List<SnapshotRequest> batch = new ArrayList<>();

        // Собираем batch запросов из очереди
        for (int i = 0; i < snapshotBatchSize && !snapshotQueue.isEmpty(); i++) {
            SnapshotRequest request = snapshotQueue.poll();
            if (request != null) {
                batch.add(request);
            }
        }

        return batch;
    }

    private void processBatchAsync(List<SnapshotRequest> batch) {
        // Ограничиваем количество одновременно выполняющихся снапшотов
        while (activeSnapshotTasks.get() >= maxConcurrentSnapshots) {
            try {
                Thread.sleep(10); // Короткое ожидание
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        activeSnapshotTasks.incrementAndGet();

        // Запускаем обработку batch в отдельном virtual thread
        snapshotExecutor.submit(() -> {
            try {
                processBatch(batch);
            } finally {
                activeSnapshotTasks.decrementAndGet();
            }
        });
    }

    /**
     * Обработка batch запросов на снапшоты
     */
    private void processBatch(List<SnapshotRequest> batch) {
        long batchStartTime = System.currentTimeMillis();

        log.debug("Processing snapshot batch of {} requests", batch.size());

        for (SnapshotRequest request : batch) {
            try {
                SnapshotResult result = createSnapshotInternal(request.accountId(), request.operationDate());
                request.resultFuture().complete(result);

            } catch (Exception e) {
                log.error("Failed to create snapshot for account {} on {}",
                    request.accountId(), request.operationDate(), e);

                request.resultFuture().completeExceptionally(
                    new SnapshotCreationException("Failed to create snapshot", e)
                );
            }
        }

        long batchDuration = System.currentTimeMillis() - batchStartTime;
        totalSnapshotTime.addAndGet(batchDuration);
        totalSnapshotsCreated.addAndGet(batch.size());

        log.debug("Snapshot batch of {} completed in {}ms", batch.size(), batchDuration);
    }

    /**
     * Создание снапшота для конкретного счета и даты
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    protected SnapshotResult createSnapshotInternal(UUID accountId, LocalDate operationDate) {
        long startTime = System.currentTimeMillis();

        try {
            // 1. Проверяем, не существует ли уже снапшот на эту дату
            Optional<EntriesSnapshot> existingSnapshot = snapshotRepository
                .findLatestSnapshotForDate(accountId, operationDate);

            if (existingSnapshot.isPresent()) {
                log.debug("Snapshot already exists for account {} on {}", accountId, operationDate);
                return new SnapshotResult(existingSnapshot.get(), false, 0, 0);
            }

            // 2. Находим последний снапшот (если есть) для оптимизации вычислений
            Optional<EntriesSnapshot> lastSnapshot = snapshotRepository
                .findLatestSnapshotBeforeDate(accountId, operationDate);

            // 3. Определяем диапазон проводок для обработки
            SnapshotRange range = calculateSnapshotRange(accountId, operationDate, lastSnapshot);

            if (range.entryCount() < minOperationsThreshold) {
                log.debug("Insufficient operations ({}) for snapshot creation, account {}",
                    range.entryCount(), accountId);
                return new SnapshotResult(null, false, range.entryCount(), 0);
            }

            // 4. Вычисляем баланс на основе диапазона проводок
            BalanceCalculation calculation = calculateBalanceInRange(accountId, range);

            // 5. Создаем новый снапшот
            EntriesSnapshot newSnapshot = createSnapshot(
                accountId,
                operationDate,
                calculation,
                range
            );

            // 6. Сохраняем снапшот
            EntriesSnapshot savedSnapshot = snapshotRepository.save(newSnapshot);

            long duration = System.currentTimeMillis() - startTime;

            log.info("Snapshot created for account {} on {}: balance={}, operations={}, duration={}ms",
                accountId, operationDate, calculation.finalBalance(),
                calculation.operationsProcessed(), duration);

            return new SnapshotResult(savedSnapshot, true, calculation.operationsProcessed(), duration);

        } catch (Exception e) {
            log.error("Failed to create snapshot for account {} on {}", accountId, operationDate, e);
            throw new SnapshotCreationException("Snapshot creation failed", e);
        }
    }

    /**
     * Вычисляем диапазон проводок для снапшота
     */
    private SnapshotRange calculateSnapshotRange(UUID accountId, LocalDate operationDate,
                                                 Optional<EntriesSnapshot> lastSnapshot) {

        Long startOrdinal = lastSnapshot.map(EntriesSnapshot::getLastEntryOrdinal).orElse(0L);

        // Находим максимальную проводку на указанную дату
        Optional<EntryRecord> maxEntry = entryRecordsRepository
            .findMaxEntryForAccountAndDate(accountId, operationDate);

        if (maxEntry.isEmpty()) {
            return new SnapshotRange(startOrdinal, startOrdinal, null, 0);
        }

        Long endOrdinal = maxEntry.get().getEntryOrdinal();

        // Подсчитываем количество проводок в диапазоне
        int entryCount = entryRecordsRepository
            .countEntriesInRange(accountId, startOrdinal + 1, endOrdinal);

        return new SnapshotRange(startOrdinal, endOrdinal, maxEntry.get().getId(), entryCount);
    }

    /**
     * Вычисляем баланс в указанном диапазоне проводок
     */
    private BalanceCalculation calculateBalanceInRange(UUID accountId, SnapshotRange range) {

        // Начальный баланс (из последнего снапшота или 0)
        long startBalance = snapshotRepository
            .findLatestSnapshotBeforeOrdinal(accountId, range.startOrdinal())
            .map(EntriesSnapshot::getBalance)
            .orElse(0L);

        // Получаем все проводки в диапазоне и вычисляем баланс
        List<EntryRecord> entries = entryRecordsRepository
            .findEntriesInOrdinalRange(accountId, range.startOrdinal() + 1, range.endOrdinal());

        long runningBalance = startBalance;
        int operationsProcessed = 0;

        for (EntryRecord entry : entries) {
            switch (entry.getEntryType()) {
                case CREDIT -> runningBalance += entry.getAmount();
                case DEBIT -> runningBalance -= entry.getAmount();
            }
            operationsProcessed++;
        }

        return new BalanceCalculation(
            startBalance,
            runningBalance,
            operationsProcessed,
            entries.isEmpty() ? null : entries.get(entries.size() - 1)
        );
    }

    /**
     * Создаем объект снапшота
     */
    private EntriesSnapshot createSnapshot(UUID accountId, LocalDate operationDate,
                                           BalanceCalculation calculation, SnapshotRange range) {

        EntriesSnapshot snapshot = new EntriesSnapshot();
        snapshot.setId(UUID.randomUUID()); // Будет заменен на generate_uuid_v7_precise() в БД
        snapshot.setAccountId(accountId);
        snapshot.setOperationDate(operationDate);
        snapshot.setBalance(calculation.finalBalance());
        snapshot.setLastEntryRecordId(range.lastEntryId());
        snapshot.setLastEntryOrdinal(range.endOrdinal());
        snapshot.setOperationsCount(calculation.operationsProcessed());
        snapshot.setCreatedAt(ZonedDateTime.now());

        return snapshot;
    }

// ===== ИНТЕЛЛЕКТУАЛЬНОЕ УПРАВЛЕНИЕ СНАПШОТАМИ ПО ТЕМПЕРАТУРЕ СЧЕТОВ =====

    @Component
    public class TemperatureAwareSnapshotService extends AsynchronousSnapshotService {

        private final MLEnabledHotAccountManager hotAccountManager;
        private final MLEnabledActivityTracker activityTracker;

        // Очереди с приоритетами для разных типов счетов
        private final PriorityBlockingQueue<PrioritizedSnapshotRequest> prioritizedQueue;

        // Конфигурация интенсивности создания снапшотов (ИСПРАВЛЕННАЯ ЛОГИКА)
        @Value("${ledger.snapshot.ultra-hot.threshold:800}")
        private int ultraHotOperationsThreshold; // Больше всего - горячий счет накапливает много операций

        @Value("${ledger.snapshot.hot.threshold:300}")
        private int hotOperationsThreshold; // Много операций

        @Value("${ledger.snapshot.warm.threshold:100}")
        private int warmOperationsThreshold; // Умеренное количество

        @Value("${ledger.snapshot.cold.threshold:20}")
        private int coldOperationsThreshold; // Мало операций - редко используемый счет

        // Интервалы создания снапшотов
        @Value("${ledger.snapshot.ultra-hot.interval-minutes:5}")
        private int ultraHotIntervalMinutes;

        @Value("${ledger.snapshot.hot.interval-minutes:30}")
        private int hotIntervalMinutes;

        @Value("${ledger.snapshot.warm.interval-minutes:120}")
        private int warmIntervalMinutes;

        @Value("${ledger.snapshot.cold.interval-minutes:1440}") // 24 hours
        private int coldIntervalMinutes;

        // Последние времена создания снапшотов для каждого типа
        private final Map<UUID, Long> lastSnapshotTimes = new ConcurrentHashMap<>();

        // Статистика по температурам
        private final AtomicLong ultraHotSnapshots = new AtomicLong(0);
        private final AtomicLong hotSnapshots = new AtomicLong(0);
        private final AtomicLong warmSnapshots = new AtomicLong(0);
        private final AtomicLong coldSnapshots = new AtomicLong(0);

        public TemperatureAwareSnapshotService(SnapshotRepository snapshotRepository,
                                               EntryRecordsRepository entryRecordsRepository,
                                               UltraHighPerformanceWAL walService,
                                               MLEnabledHotAccountManager hotAccountManager,
                                               MLEnabledActivityTracker activityTracker) {
            super(snapshotRepository, entryRecordsRepository, walService);
            this.hotAccountManager = hotAccountManager;
            this.activityTracker = activityTracker;

            // Приоритетная очередь: чем выше температура, тем выше приоритет
            this.prioritizedQueue = new PriorityBlockingQueue<>(1000,
                Comparator.comparing(PrioritizedSnapshotRequest::priority).reversed()
                    .thenComparing(PrioritizedSnapshotRequest::requestTime));

            log.info("Temperature-Aware Snapshot Service initialized with thresholds: " +
                    "ultra-hot={}, hot={}, warm={}, cold={}",
                ultraHotOperationsThreshold, hotOperationsThreshold,
                warmOperationsThreshold, coldOperationsThreshold);
        }

        /**
         * Умный запрос на создание снапшота с учетом температуры
         */
        @Override
        public CompletableFuture<SnapshotResult> requestSnapshot(UUID accountId, LocalDate operationDate) {
            // Определяем температуру счета и приоритет
            AccountTemperature temperature = determineAccountTemperature(accountId);
            int operationsThreshold = getOperationsThreshold(temperature);

            // Проверяем, нужен ли снапшот на основе температуры
            if (!shouldCreateSnapshot(accountId, temperature, operationDate)) {
                log.debug("Snapshot not needed for {} account {} (too early)", temperature, accountId);
                return CompletableFuture.completedFuture(
                    new SnapshotResult(null, false, 0, 0)
                );
            }

            PrioritizedSnapshotRequest request = new PrioritizedSnapshotRequest(
                accountId,
                operationDate,
                temperature,
                getTemperaturePriority(temperature),
                operationsThreshold,
                System.currentTimeMillis(),
                new CompletableFuture<>()
            );

            prioritizedQueue.offer(request);

            log.debug("Prioritized snapshot requested for {} account {} with threshold {}",
                temperature, accountId, operationsThreshold);

            return request.resultFuture();
        }

        private AccountTemperature determineAccountTemperature(UUID accountId) {
            double intensity = activityTracker.getCurrentIntensity(accountId.toString());
            ProcessingStrategy strategy = hotAccountManager.assignStrategy(accountId.toString(), intensity);

            return switch (strategy) {
                case ULTRA_HOT -> AccountTemperature.ULTRA_HOT;
                case HOT -> AccountTemperature.HOT;
                case WARM -> AccountTemperature.WARM;
                case COLD -> AccountTemperature.COLD;
            };
        }

        private int getOperationsThreshold(AccountTemperature temperature) {
            return switch (temperature) {
                case ULTRA_HOT -> ultraHotOperationsThreshold;
                case HOT -> hotOperationsThreshold;
                case WARM -> warmOperationsThreshold;
                case COLD -> coldOperationsThreshold;
            };
        }

        private int getTemperaturePriority(AccountTemperature temperature) {
            return switch (temperature) {
                case ULTRA_HOT -> 1000; // Наивысший приоритет
                case HOT -> 100;
                case WARM -> 10;
                case COLD -> 1; // Низший приоритет
            };
        }

        private boolean shouldCreateSnapshot(UUID accountId, AccountTemperature temperature, LocalDate operationDate) {
            Long lastSnapshotTime = lastSnapshotTimes.get(accountId);
            if (lastSnapshotTime == null) {
                return true; // Первый снапшот всегда создаем
            }

            long currentTime = System.currentTimeMillis();
            long intervalMs = getSnapshotIntervalMs(temperature);

            return (currentTime - lastSnapshotTime) >= intervalMs;
        }

        private long getSnapshotIntervalMs(AccountTemperature temperature) {
            int intervalMinutes = switch (temperature) {
                case ULTRA_HOT -> ultraHotIntervalMinutes;
                case HOT -> hotIntervalMinutes;
                case WARM -> warmIntervalMinutes;
                case COLD -> coldIntervalMinutes;
            };
            return TimeUnit.MINUTES.toMillis(intervalMinutes);
        }

        /**
         * Переопределенный процессор очереди с приоритетами
         */
        @Override
        protected void processSnapshotQueue() {
            log.info("Temperature-aware snapshot queue processor started");

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    List<PrioritizedSnapshotRequest> batch = collectPrioritizedBatch();

                    if (!batch.isEmpty()) {
                        processPrioritizedBatch(batch);
                    } else {
                        Thread.sleep(100);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error in temperature-aware snapshot processor", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        private List<PrioritizedSnapshotRequest> collectPrioritizedBatch() throws InterruptedException {
            List<PrioritizedSnapshotRequest> batch = new ArrayList<>();

            // Берем первый элемент блокирующе
            PrioritizedSnapshotRequest first = prioritizedQueue.poll(100, TimeUnit.MILLISECONDS);
            if (first != null) {
                batch.add(first);

                // Добавляем остальные элементы того же приоритета
                while (batch.size() < snapshotBatchSize) {
                    PrioritizedSnapshotRequest next = prioritizedQueue.peek();
                    if (next != null && next.priority() >= first.priority() * 0.1) { // В пределах порядка
                        next = prioritizedQueue.poll();
                        if (next != null) {
                            batch.add(next);
                        }
                    } else {
                        break;
                    }
                }
            }

            return batch;
        }

        private void processPrioritizedBatch(List<PrioritizedSnapshotRequest> batch) {
            activeSnapshotTasks.incrementAndGet();

            snapshotExecutor.submit(() -> {
                try {
                    processPrioritizedBatchInternal(batch);
                } finally {
                    activeSnapshotTasks.decrementAndGet();
                }
            });
        }

        private void processPrioritizedBatchInternal(List<PrioritizedSnapshotRequest> batch) {
            long batchStartTime = System.currentTimeMillis();

            // Группируем по температуре для статистики
            Map<AccountTemperature, List<PrioritizedSnapshotRequest>> byTemperature = batch.stream()
                .collect(Collectors.groupingBy(PrioritizedSnapshotRequest::temperature));

            log.debug("Processing prioritized batch: {}",
                byTemperature.entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue().size())
                    .collect(Collectors.joining(", ")));

            for (PrioritizedSnapshotRequest request : batch) {
                try {
                    SnapshotResult result = createTemperatureAwareSnapshot(request);
                    request.resultFuture().complete(result);

                    // Обновляем время последнего снапшота
                    lastSnapshotTimes.put(request.accountId(), System.currentTimeMillis());

                    // Обновляем статистику
                    updateTemperatureStatistics(request.temperature());

                } catch (Exception e) {
                    log.error("Failed to create temperature-aware snapshot for {} account {}",
                        request.temperature(), request.accountId(), e);

                    request.resultFuture().completeExceptionally(
                        new SnapshotCreationException("Temperature-aware snapshot creation failed", e)
                    );
                }
            }

            long batchDuration = System.currentTimeMillis() - batchStartTime;
            log.debug("Temperature-aware batch of {} completed in {}ms", batch.size(), batchDuration);
        }

        @Transactional(propagation = Propagation.REQUIRES_NEW)
        protected SnapshotResult createTemperatureAwareSnapshot(PrioritizedSnapshotRequest request) {
            long startTime = System.currentTimeMillis();

            try {
                // Проверяем существующий снапшот
                Optional<EntriesSnapshot> existingSnapshot = snapshotRepository
                    .findLatestSnapshotForDate(request.accountId(), request.operationDate());

                if (existingSnapshot.isPresent()) {
                    return new SnapshotResult(existingSnapshot.get(), false, 0, 0);
                }

                // Находим последний снапшот
                Optional<EntriesSnapshot> lastSnapshot = snapshotRepository
                    .findLatestSnapshotBeforeDate(request.accountId(), request.operationDate());

                // Вычисляем диапазон с учетом температуры
                SnapshotRange range = calculateTemperatureAwareRange(
                    request.accountId(),
                    request.operationDate(),
                    lastSnapshot,
                    request.temperature()
                );

                // Проверяем порог операций для данной температуры
                if (range.entryCount() < request.operationsThreshold()) {
                    log.debug("Insufficient operations ({}/{}) for {} snapshot creation",
                        range.entryCount(), request.operationsThreshold(), request.temperature());
                    return new SnapshotResult(null, false, range.entryCount(), 0);
                }

                // Вычисляем баланс
                BalanceCalculation calculation = calculateBalanceInRange(request.accountId(), range);

                // Создаем снапшот с метаданными температуры
                EntriesSnapshot newSnapshot = createTemperatureAwareSnapshot(
                    request.accountId(),
                    request.operationDate(),
                    calculation,
                    range,
                    request.temperature()
                );

                EntriesSnapshot savedSnapshot = snapshotRepository.save(newSnapshot);

                long duration = System.currentTimeMillis() - startTime;

                log.info("Temperature-aware snapshot created for {} account {}: balance={}, operations={}, duration={}ms",
                    request.temperature(), request.accountId(), calculation.finalBalance(),
                    calculation.operationsProcessed(), duration);

                return new SnapshotResult(savedSnapshot, true, calculation.operationsProcessed(), duration);

            } catch (Exception e) {
                log.error("Failed to create temperature-aware snapshot for {} account {}",
                    request.temperature(), request.accountId(), e);
                throw new SnapshotCreationException("Temperature-aware snapshot creation failed", e);
            }
        }

        private SnapshotRange calculateTemperatureAwareRange(UUID accountId, LocalDate operationDate,
                                                             Optional<EntriesSnapshot> lastSnapshot,
                                                             AccountTemperature temperature) {

            Long startOrdinal = lastSnapshot.map(EntriesSnapshot::getLastEntryOrdinal).orElse(0L);

            // Для горячих счетов берем более свежие данные
            ZonedDateTime cutoffTime = switch (temperature) {
                case ULTRA_HOT -> ZonedDateTime.now(); // Самые свежие данные
                case HOT -> ZonedDateTime.now().minusMinutes(1);
                case WARM -> ZonedDateTime.now().minusMinutes(5);
                case COLD -> operationDate.plusDays(1).atStartOfDay().atZone(ZoneOffset.UTC);
            };

            // Находим максимальную проводку с учетом температуры
            Optional<EntryRecord> maxEntry = entryRecordsRepository
                .findMaxEntryForAccountBeforeTime(accountId, cutoffTime);

            if (maxEntry.isEmpty()) {
                return new SnapshotRange(startOrdinal, startOrdinal, null, 0);
            }

            Long endOrdinal = maxEntry.get().getEntryOrdinal();

            int entryCount = entryRecordsRepository
                .countEntriesInRange(accountId, startOrdinal + 1, endOrdinal);

            return new SnapshotRange(startOrdinal, endOrdinal, maxEntry.get().getId(), entryCount);
        }

        private EntriesSnapshot createTemperatureAwareSnapshot(UUID accountId, LocalDate operationDate,
                                                               BalanceCalculation calculation, SnapshotRange range,
                                                               AccountTemperature temperature) {

            EntriesSnapshot snapshot = new EntriesSnapshot();
            snapshot.setId(UUID.randomUUID());
            snapshot.setAccountId(accountId);
            snapshot.setOperationDate(operationDate);
            snapshot.setBalance(calculation.finalBalance());
            snapshot.setLastEntryRecordId(range.lastEntryId());
            snapshot.setLastEntryOrdinal(range.endOrdinal());
            snapshot.setOperationsCount(calculation.operationsProcessed());
            snapshot.setCreatedAt(ZonedDateTime.now());

            // Можно добавить метаданные о температуре в отдельное поле, если нужно

            return snapshot;
        }

        private void updateTemperatureStatistics(AccountTemperature temperature) {
            switch (temperature) {
                case ULTRA_HOT -> ultraHotSnapshots.incrementAndGet();
                case HOT -> hotSnapshots.incrementAndGet();
                case WARM -> warmSnapshots.incrementAndGet();
                case COLD -> coldSnapshots.incrementAndGet();
            }
        }

        /**
         * Переопределенный периодический триггер с учетом температуры
         */
        @Override
        @Scheduled(fixedRate = 60000) // Каждую минуту
        public void triggerPeriodicSnapshots() {
            if (!enablePeriodicSnapshots) return;

            LocalDate today = LocalDate.now();

            // Находим активные счета с группировкой по температуре
            List<UUID> activeAccounts = entryRecordsRepository
                .findActiveAccountsSince(ZonedDateTime.now().minusHours(1));

            Map<AccountTemperature, List<UUID>> accountsByTemperature = activeAccounts.stream()
                .collect(Collectors.groupingBy(this::determineAccountTemperature));

            log.debug("Periodic snapshots: ultra-hot={}, hot={}, warm={}, cold={}",
                accountsByTemperature.getOrDefault(AccountTemperature.ULTRA_HOT, List.of()).size(),
                accountsByTemperature.getOrDefault(AccountTemperature.HOT, List.of()).size(),
                accountsByTemperature.getOrDefault(AccountTemperature.WARM, List.of()).size(),
                accountsByTemperature.getOrDefault(AccountTemperature.COLD, List.of()).size());

            // Обрабатываем каждую температурную группу
            for (Map.Entry<AccountTemperature, List<UUID>> entry : accountsByTemperature.entrySet()) {
                AccountTemperature temperature = entry.getKey();
                List<UUID> accounts = entry.getValue();

                // Ограничиваем количество снапшотов для каждой температуры
                int maxSnapshots = getMaxPeriodicSnapshots(temperature);
                accounts.stream()
                    .limit(maxSnapshots)
                    .forEach(accountId -> requestSnapshot(accountId, today));
            }
        }

        private int getMaxPeriodicSnapshots(AccountTemperature temperature) {
            return switch (temperature) {
                case ULTRA_HOT -> 100;  // Много снапшотов для горячих счетов
                case HOT -> 50;
                case WARM -> 20;
                case COLD -> 5;         // Мало снапшотов для холодных счетов
            };
        }

        /**
         * Статистика с разбивкой по температуре
         */
        public TemperatureAwareSnapshotStatistics getTemperatureStatistics() {
            return new TemperatureAwareSnapshotStatistics(
                getStatistics(), // Базовая статистика
                ultraHotSnapshots.get(),
                hotSnapshots.get(),
                warmSnapshots.get(),
                coldSnapshots.get(),
                prioritizedQueue.size(),
                lastSnapshotTimes.size()
            );
        }
    }

// ===== ДОПОЛНИТЕЛЬНЫЕ ТИПЫ И КЛАССЫ =====

    public enum AccountTemperature {
        ULTRA_HOT, HOT, WARM, COLD
    }

    public record PrioritizedSnapshotRequest(
        UUID accountId,
        LocalDate operationDate,
        AccountTemperature temperature,
        int priority,
        int operationsThreshold,
        long requestTime,
        CompletableFuture<SnapshotResult> resultFuture
    ) {}

    public record TemperatureAwareSnapshotStatistics(
        SnapshotStatistics baseStats,
        long ultraHotSnapshots,
        long hotSnapshots,
        long warmSnapshots,
        long coldSnapshots,
        int priorityQueueSize,
        int trackedAccountsCount
    ) {}

    // Добавляем в EntryRecordsRepository новый метод
    @Repository
    public interface EntryRecordsRepository extends JpaRepository<EntryRecord, UUID> {

        // ... существующие методы ...

        // Поиск максимальной проводки до определенного времени
        @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.createdAt < :beforeTime
        ORDER BY e.entryOrdinal DESC
        LIMIT 1
        """)
        Optional<EntryRecord> findMaxEntryForAccountBeforeTime(@Param("accountId") UUID accountId,
                                                               @Param("beforeTime") ZonedDateTime beforeTime);
    }

    private boolean isSnapshotInProgress(UUID accountId, LocalDate date) {
        // Простая проверка - есть ли запрос в очереди
        return snapshotQueue.stream()
            .anyMatch(req -> req.accountId().equals(accountId) && req.operationDate().equals(date));
    }

    /**
     * Получение статистики работы снапшотов
     */
    public SnapshotStatistics getStatistics() {
        long totalSnapshots = totalSnapshotsCreated.get();
        long totalTime = totalSnapshotTime.get();
        double avgTimePerSnapshot = totalSnapshots > 0 ? (double) totalTime / totalSnapshots : 0.0;

        return new SnapshotStatistics(
            totalSnapshots,
            snapshotQueue.size(),
            activeSnapshotTasks.get(),
            avgTimePerSnapshot,
            System.currentTimeMillis()
        );
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Asynchronous Snapshot Service...");
        snapshotExecutor.shutdown();

        try {
            if (!snapshotExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                snapshotExecutor.shutdownNow();
                log.warn("Snapshot executor did not terminate gracefully");
            }
        } catch (InterruptedException e) {
            snapshotExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("Asynchronous Snapshot Service shutdown completed");
    }
}

// ===== 2. БЫСТРОЕ ВЫЧИСЛЕНИЕ БАЛАНСОВ С ИСПОЛЬЗОВАНИЕМ СНАПШОТОВ =====

@Service
@Transactional(readOnly = true)
public class OptimizedBalanceService {

    private final SnapshotRepository snapshotRepository;
    private final EntryRecordsRepository entryRecordsRepository;
    private final AsynchronousSnapshotService snapshotService;

    // Кэш балансов для горячих счетов
    private final Map<UUID, CachedBalance> balanceCache = new ConcurrentHashMap<>();

    // Статистика производительности
    private final AtomicLong fastBalanceQueries = new AtomicLong(0);
    private final AtomicLong slowBalanceQueries = new AtomicLong(0);
    private final AtomicLong totalBalanceTime = new AtomicLong(0);

    public OptimizedBalanceService(SnapshotRepository snapshotRepository,
                                   EntryRecordsRepository entryRecordsRepository,
                                   AsynchronousSnapshotService snapshotService) {
        this.snapshotRepository = snapshotRepository;
        this.entryRecordsRepository = entryRecordsRepository;
        this.snapshotService = snapshotService;
    }

    /**
     * Быстрое получение баланса с использованием снапшотов
     */
    public CompletableFuture<AccountBalance> getBalance(UUID accountId, LocalDate asOfDate) {
        long startTime = System.nanoTime();

        // 1. Проверяем кэш горячих счетов
        CachedBalance cached = balanceCache.get(accountId);
        if (cached != null && cached.isValid(asOfDate)) {
            fastBalanceQueries.incrementAndGet();

            return CompletableFuture.completedFuture(
                new AccountBalance(accountId, cached.balance(), cached.lastUpdated())
            );
        }

        // 2. Ищем ближайший снапшот
        return findBestSnapshotAsync(accountId, asOfDate)
            .thenCompose(snapshotOpt -> {
                if (snapshotOpt.isPresent()) {
                    EntriesSnapshot snapshot = snapshotOpt.get();

                    // Быстрый путь: снапшот актуален
                    if (snapshot.getOperationDate().equals(asOfDate)) {
                        fastBalanceQueries.incrementAndGet();
                        return CompletableFuture.completedFuture(
                            new AccountBalance(accountId, snapshot.getBalance(), snapshot.getCreatedAt())
                        );
                    }

                    // Вычисляем баланс от снапшота до нужной даты
                    return calculateBalanceFromSnapshot(accountId, snapshot, asOfDate);
                } else {
                    // Медленный путь: вычисляем с нуля
                    return calculateBalanceFromScratch(accountId, asOfDate);
                }
            })
            .whenComplete((balance, throwable) -> {
                long duration = System.nanoTime() - startTime;
                totalBalanceTime.addAndGet(duration);

                if (balance != null) {
                    // Кэшируем результат для горячих счетов
                    cacheBalance(accountId, balance);

                    // Асинхронно создаем снапшот если его нет
                    if (throwable == null) {
                        triggerSnapshotIfNeeded(accountId, asOfDate);
                    }
                }
            });
    }

    private CompletableFuture<Optional<EntriesSnapshot>> findBestSnapshotAsync(UUID accountId, LocalDate asOfDate) {
        return CompletableFuture.supplyAsync(() ->
            snapshotRepository.findLatestSnapshotBeforeOrOnDate(accountId, asOfDate)
        );
    }

    private CompletableFuture<AccountBalance> calculateBalanceFromSnapshot(UUID accountId,
                                                                           EntriesSnapshot snapshot,
                                                                           LocalDate asOfDate) {
        slowBalanceQueries.incrementAndGet();

        return CompletableFuture.supplyAsync(() -> {
            // Получаем проводки после снапшота до нужной даты
            List<EntryRecord> entriesAfterSnapshot = entryRecordsRepository
                .findEntriesAfterOrdinalAndBeforeDate(
                    accountId,
                    snapshot.getLastEntryOrdinal(),
                    asOfDate.plusDays(1).atStartOfDay().atZone(ZoneOffset.UTC)
                );

            long balance = snapshot.getBalance();

            // Применяем проводки к балансу снапшота
            for (EntryRecord entry : entriesAfterSnapshot) {
                switch (entry.getEntryType()) {
                    case CREDIT -> balance += entry.getAmount();
                    case DEBIT -> balance -= entry.getAmount();
                }
            }

            return new AccountBalance(accountId, balance, ZonedDateTime.now());
        });
    }

    private CompletableFuture<AccountBalance> calculateBalanceFromScratch(UUID accountId, LocalDate asOfDate) {
        slowBalanceQueries.incrementAndGet();

        return CompletableFuture.supplyAsync(() -> {
            // Получаем все проводки до указанной даты
            List<EntryRecord> allEntries = entryRecordsRepository
                .findAllEntriesBeforeDate(
                    accountId,
                    asOfDate.plusDays(1).atStartOfDay().atZone(ZoneOffset.UTC)
                );

            long balance = 0L;

            for (EntryRecord entry : allEntries) {
                switch (entry.getEntryType()) {
                    case CREDIT -> balance += entry.getAmount();
                    case DEBIT -> balance -= entry.getAmount();
                }
            }

            return new AccountBalance(accountId, balance, ZonedDateTime.now());
        });
    }

    private void cacheBalance(UUID accountId, AccountBalance balance) {
        // Кэшируем баланс только для горячих счетов (ограничиваем размер кэша)
        if (balanceCache.size() < 10000) {
            balanceCache.put(accountId, new CachedBalance(
                balance.balance(),
                balance.lastUpdated(),
                System.currentTimeMillis()
            ));
        }
    }

    private void triggerSnapshotIfNeeded(UUID accountId, LocalDate asOfDate) {
        // Проверяем, нужен ли снапшот для оптимизации будущих запросов
        Optional<EntriesSnapshot> lastSnapshot = snapshotRepository
            .findLatestSnapshotForAccount(accountId);

        boolean needsSnapshot = lastSnapshot.isEmpty() ||
            lastSnapshot.get().getOperationDate().isBefore(asOfDate.minusDays(1));

        if (needsSnapshot) {
            snapshotService.requestSnapshot(accountId, asOfDate)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.debug("Background snapshot creation failed for account {}: {}",
                            accountId, throwable.getMessage());
                    } else {
                        log.debug("Background snapshot created for account {}: operations={}",
                            accountId, result.operationsProcessed());
                    }
                });
        }
    }

    /**
     * Получение статистики производительности
     */
    public BalanceServiceStatistics getStatistics() {
        long totalQueries = fastBalanceQueries.get() + slowBalanceQueries.get();
        double fastQueryPercentage = totalQueries > 0 ?
            (double) fastBalanceQueries.get() / totalQueries * 100.0 : 0.0;

        double avgQueryTimeMs = totalQueries > 0 ?
            (double) totalBalanceTime.get() / totalQueries / 1_000_000.0 : 0.0;

        return new BalanceServiceStatistics(
            fastBalanceQueries.get(),
            slowBalanceQueries.get(),
            fastQueryPercentage,
            avgQueryTimeMs,
            balanceCache.size()
        );
    }

    // Периодическая очистка кэша
    @Scheduled(fixedRate = 300000) // 5 минут
    public void cleanupCache() {
        long cutoffTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(10);

        balanceCache.entrySet().removeIf(entry ->
            entry.getValue().cachedAt() < cutoffTime
        );
    }
}

// ===== 3. ENTITY КЛАССЫ =====

@Entity
@Table(name = "entries_snapshots", schema = "ledger")
public class EntriesSnapshot {
    @Id
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "operation_date", nullable = false)
    private LocalDate operationDate;

    @Column(name = "balance", nullable = false)
    private Long balance;

    @Column(name = "last_entry_record_id", nullable = false)
    private UUID lastEntryRecordId;

    @Column(name = "last_entry_ordinal", nullable = false)
    private Long lastEntryOrdinal;

    @Column(name = "operations_count", nullable = false)
    private Integer operationsCount;

    @Column(name = "snapshot_ordinal", nullable = false)
    private Long snapshotOrdinal;

    @Column(name = "created_at", nullable = false)
    private ZonedDateTime createdAt;

    // Constructors, getters, setters...
    public EntriesSnapshot() {}

    // Getters and setters
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getAccountId() { return accountId; }
    public void setAccountId(UUID accountId) { this.accountId = accountId; }

    public LocalDate getOperationDate() { return operationDate; }
    public void setOperationDate(LocalDate operationDate) { this.operationDate = operationDate; }

    public Long getBalance() { return balance; }
    public void setBalance(Long balance) { this.balance = balance; }

    public UUID getLastEntryRecordId() { return lastEntryRecordId; }
    public void setLastEntryRecordId(UUID lastEntryRecordId) { this.lastEntryRecordId = lastEntryRecordId; }

    public Long getLastEntryOrdinal() { return lastEntryOrdinal; }
    public void setLastEntryOrdinal(Long lastEntryOrdinal) { this.lastEntryOrdinal = lastEntryOrdinal; }

    public Integer getOperationsCount() { return operationsCount; }
    public void setOperationsCount(Integer operationsCount) { this.operationsCount = operationsCount; }

    public Long getSnapshotOrdinal() { return snapshotOrdinal; }
    public void setSnapshotOrdinal(Long snapshotOrdinal) { this.snapshotOrdinal = snapshotOrdinal; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }
}

@Entity
@Table(name = "entry_records", schema = "ledger")
public class EntryRecord {
    @Id
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;

    @Enumerated(EnumType.STRING)
    @Column(name = "entry_type", nullable = false, length = 6)
    private EntryType entryType;

    @Column(name = "amount", nullable = false)
    private Long amount;

    @Column(name = "created_at", nullable = false)
    private ZonedDateTime createdAt;

    @Column(name = "operation_date", nullable = false)
    private LocalDate operationDate;

    @Column(name = "idempotency_key")
    private UUID idempotencyKey;

    @Column(name = "currency_code", nullable = false, length = 8)
    private String currencyCode;

    @Column(name = "entry_ordinal", nullable = false)
    private Long entryOrdinal;

    // Constructors, getters, setters...
    public EntryRecord() {}

    // Getters and setters...
    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getAccountId() { return accountId; }
    public void setAccountId(UUID accountId) { this.accountId = accountId; }

    public UUID getTransactionId() { return transactionId; }
    public void setTransactionId(UUID transactionId) { this.transactionId = transactionId; }

    public EntryType getEntryType() { return entryType; }
    public void setEntryType(EntryType entryType) { this.entryType = entryType; }

    public Long getAmount() { return amount; }
    public void setAmount(Long amount) { this.amount = amount; }

    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }

    public LocalDate getOperationDate() { return operationDate; }
    public void setOperationDate(LocalDate operationDate) { this.operationDate = operationDate; }

    public UUID getIdempotencyKey() { return idempotencyKey; }
    public void setIdempotencyKey(UUID idempotencyKey) { this.idempotencyKey = idempotencyKey; }

    public String getCurrencyCode() { return currencyCode; }
    public void setCurrencyCode(String currencyCode) { this.currencyCode = currencyCode; }

    public Long getEntryOrdinal() { return entryOrdinal; }
    public void setEntryOrdinal(Long entryOrdinal) { this.entryOrdinal = entryOrdinal; }
}

// ===== 4. REPOSITORY ИНТЕРФЕЙСЫ =====

@Repository
public interface SnapshotRepository extends JpaRepository<EntriesSnapshot, UUID> {

    // Поиск последнего снапшота для счета и даты
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.accountId = :accountId 
          AND s.operationDate = :operationDate
        ORDER BY s.snapshotOrdinal DESC
        LIMIT 1
        """)
    Optional<EntriesSnapshot> findLatestSnapshotForDate(@Param("accountId") UUID accountId,
                                                        @Param("operationDate") LocalDate operationDate);

    // Поиск последнего снапшота до указанной даты
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.accountId = :accountId 
          AND s.operationDate < :beforeDate
        ORDER BY s.operationDate DESC, s.snapshotOrdinal DESC
        LIMIT 1
        """)
    Optional<EntriesSnapshot> findLatestSnapshotBeforeDate(@Param("accountId") UUID accountId,
                                                           @Param("beforeDate") LocalDate beforeDate);

    // Поиск последнего снапшота до или на указанную дату
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.accountId = :accountId 
          AND s.operationDate <= :onOrBeforeDate
        ORDER BY s.operationDate DESC, s.snapshotOrdinal DESC
        LIMIT 1
        """)
    Optional<EntriesSnapshot> findLatestSnapshotBeforeOrOnDate(@Param("accountId") UUID accountId,
                                                               @Param("onOrBeforeDate") LocalDate onOrBeforeDate);

    // Поиск последнего снапшота до указанного ordinal
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.accountId = :accountId 
          AND s.lastEntryOrdinal <= :maxOrdinal
        ORDER BY s.lastEntryOrdinal DESC
        LIMIT 1
        """)
    Optional<EntriesSnapshot> findLatestSnapshotBeforeOrdinal(@Param("accountId") UUID accountId,
                                                              @Param("maxOrdinal") Long maxOrdinal);

    // Поиск последнего снапшота для счета
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.accountId = :accountId
        ORDER BY s.operationDate DESC, s.snapshotOrdinal DESC
        LIMIT 1
        """)
    Optional<EntriesSnapshot> findLatestSnapshotForAccount(@Param("accountId") UUID accountId);

    // Поиск старых снапшотов для очистки
    @Query("""
        SELECT s FROM EntriesSnapshot s 
        WHERE s.createdAt < :cutoffDate
        ORDER BY s.createdAt ASC
        """)
    List<EntriesSnapshot> findOldSnapshotsForCleanup(@Param("cutoffDate") ZonedDateTime cutoffDate,
                                                     Pageable pageable);

    // Статистика снапшотов
    @Query("""
        SELECT COUNT(s), AVG(s.operationsCount), MAX(s.operationsCount)
        FROM EntriesSnapshot s 
        WHERE s.createdAt >= :since
        """)
    Object[] getSnapshotStatisticsSince(@Param("since") ZonedDateTime since);
}

@Repository
public interface EntryRecordsRepository extends JpaRepository<EntryRecord, UUID> {

    // Поиск максимальной проводки для счета и даты
    @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.operationDate = :operationDate
        ORDER BY e.entryOrdinal DESC
        LIMIT 1
        """)
    Optional<EntryRecord> findMaxEntryForAccountAndDate(@Param("accountId") UUID accountId,
                                                        @Param("operationDate") LocalDate operationDate);

    // Подсчет проводок в диапазоне ordinal
    @Query("""
        SELECT COUNT(e) FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.entryOrdinal > :startOrdinal 
          AND e.entryOrdinal <= :endOrdinal
        """)
    int countEntriesInRange(@Param("accountId") UUID accountId,
                            @Param("startOrdinal") Long startOrdinal,
                            @Param("endOrdinal") Long endOrdinal);

    // Получение проводок в диапазоне ordinal
    @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.entryOrdinal > :startOrdinal 
          AND e.entryOrdinal <= :endOrdinal
        ORDER BY e.entryOrdinal ASC
        """)
    List<EntryRecord> findEntriesInOrdinalRange(@Param("accountId") UUID accountId,
                                                @Param("startOrdinal") Long startOrdinal,
                                                @Param("endOrdinal") Long endOrdinal);

    // Получение проводок после ordinal и до даты
    @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.entryOrdinal > :afterOrdinal 
          AND e.createdAt < :beforeDate
        ORDER BY e.entryOrdinal ASC
        """)
    List<EntryRecord> findEntriesAfterOrdinalAndBeforeDate(@Param("accountId") UUID accountId,
                                                           @Param("afterOrdinal") Long afterOrdinal,
                                                           @Param("beforeDate") ZonedDateTime beforeDate);

    // Получение всех проводок до даты
    @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId 
          AND e.createdAt < :beforeDate
        ORDER BY e.entryOrdinal ASC
        """)
    List<EntryRecord> findAllEntriesBeforeDate(@Param("accountId") UUID accountId,
                                               @Param("beforeDate") ZonedDateTime beforeDate);

    // Поиск активных счетов
    @Query("""
        SELECT DISTINCT e.accountId 
        FROM EntryRecord e 
        WHERE e.createdAt >= :since
        """)
    List<UUID> findActiveAccountsSince(@Param("since") ZonedDateTime since);

    // Получение последних проводок для счета
    @Query("""
        SELECT e FROM EntryRecord e 
        WHERE e.accountId = :accountId
        ORDER BY e.entryOrdinal DESC
        LIMIT :limit
        """)
    List<EntryRecord> findLatestEntriesForAccount(@Param("accountId") UUID accountId,
                                                  @Param("limit") int limit);
}

// ===== 5. DATA TRANSFER OBJECTS =====

public enum EntryType {
    DEBIT, CREDIT
}

// Запрос на создание снапшота
public record SnapshotRequest(
    UUID accountId,
    LocalDate operationDate,
    long requestTime,
    CompletableFuture<SnapshotResult> resultFuture
) {}

// Результат создания снапшота
public record SnapshotResult(
    EntriesSnapshot snapshot,
    boolean wasCreated,
    int operationsProcessed,
    long processingTimeMs
) {}

// Диапазон проводок для снапшота
public record SnapshotRange(
    Long startOrdinal,
    Long endOrdinal,
    UUID lastEntryId,
    int entryCount
) {}

// Расчет баланса
public record BalanceCalculation(
    long startBalance,
    long finalBalance,
    int operationsProcessed,
    EntryRecord lastEntry
) {}

// Кэшированный баланс
public record CachedBalance(
    long balance,
    ZonedDateTime lastUpdated,
    long cachedAt
) {
    public boolean isValid(LocalDate asOfDate) {
        // Кэш валиден если:
        // 1. Дата запроса не превышает дату кэша
        // 2. Кэш не старше 5 минут
        long maxAge = TimeUnit.MINUTES.toMillis(5);
        boolean isNotTooOld = (System.currentTimeMillis() - cachedAt) < maxAge;
        boolean isNotFuture = !asOfDate.isAfter(lastUpdated.toLocalDate());

        return isNotTooOld && isNotFuture;
    }
}

// Баланс счета
public record AccountBalance(
    UUID accountId,
    long balance,
    ZonedDateTime lastUpdated
) {}

// Статистика снапшотов
public record SnapshotStatistics(
    long totalSnapshotsCreated,
    int queueSize,
    int activeSnapshots,
    double avgProcessingTimeMs,
    long timestamp
) {}

// Статистика сервиса балансов
public record BalanceServiceStatistics(
    long fastQueries,
    long slowQueries,
    double fastQueryPercentage,
    double avgQueryTimeMs,
    int cacheSize
) {}

// ===== 6. ИСКЛЮЧЕНИЯ =====

public class SnapshotCreationException extends RuntimeException {
    public SnapshotCreationException(String message) {
        super(message);
    }

    public SnapshotCreationException(String message, Throwable cause) {
        super(message, cause);
    }
}

// ===== 7. ИНТЕГРАЦИЯ С ОСНОВНЫМ СЕРВИСОМ =====

@Component
public class SnapshotAwareLedgerService extends MLOptimizedLedgerService {

    private final OptimizedBalanceService balanceService;
    private final AsynchronousSnapshotService snapshotService;

    public SnapshotAwareLedgerService(UltraHighPerformanceWAL wal,
                                      MLEnabledActivityTracker mlTracker,
                                      MLEnabledHotAccountManager mlHotAccountManager,
                                      AccountBalanceManager balanceManager,
                                      OptimizedBalanceService balanceService,
                                      AsynchronousSnapshotService snapshotService) {
        super(wal, mlTracker, mlHotAccountManager, balanceManager);
        this.balanceService = balanceService;
        this.snapshotService = snapshotService;
    }

    /**
     * Получение баланса с использованием снапшотов
     */
    public CompletableFuture<AccountBalance> getAccountBalance(UUID accountId) {
        return getAccountBalance(accountId, LocalDate.now());
    }

    public CompletableFuture<AccountBalance> getAccountBalance(UUID accountId, LocalDate asOfDate) {
        return balanceService.getBalance(accountId, asOfDate);
    }

    /**
     * Обработка транзакции с автоматическим созданием снапшотов
     */
    @Override
    public CompletableFuture<TransactionResult> processTransaction(String accountId, Transaction transaction) {
        UUID accountUUID = UUID.fromString(accountId);

        // Обрабатываем транзакцию как обычно
        return super.processTransaction(accountId, transaction)
            .whenComplete((result, throwable) -> {
                if (result != null && result.isSuccess()) {
                    // После успешной обработки - асинхронно создаем записи в entry_records
                    createEntryRecords(accountUUID, transaction, result)
                        .whenComplete((entries, entryError) -> {
                            if (entryError == null) {
                                // После создания проводок - триггерим создание снапшота если нужно
                                triggerSnapshotIfNeeded(accountUUID, transaction.getOperationDate());
                            } else {
                                log.error("Failed to create entry records for transaction {}",
                                    transaction.getId(), entryError);
                            }
                        });
                }
            });
    }

    /**
     * Создание двойных проводок в entry_records
     */
    private CompletableFuture<List<EntryRecord>> createEntryRecords(UUID accountId,
                                                                    Transaction transaction,
                                                                    TransactionResult result) {
        return CompletableFuture.supplyAsync(() -> {
            List<EntryRecord> entries = new ArrayList<>();

            // Создаем дебетовую проводку
            if (transaction.getType() == TransactionType.DEBIT ||
                transaction.getType() == TransactionType.TRANSFER_OUT) {

                EntryRecord debitEntry = new EntryRecord();
                debitEntry.setAccountId(accountId);
                debitEntry.setTransactionId(UUID.fromString(transaction.getId()));
                debitEntry.setEntryType(EntryType.DEBIT);
                debitEntry.setAmount(transaction.getAmount().longValue() * 100); // Convert to cents
                debitEntry.setOperationDate(LocalDate.from(transaction.getTimestamp()));
                debitEntry.setIdempotencyKey(UUID.randomUUID()); // Should be from transaction
                debitEntry.setCurrencyCode("USD"); // Should be from transaction
                debitEntry.setCreatedAt(ZonedDateTime.now());

                entries.add(debitEntry);
            }

            // Создаем кредитовую проводку
            if (transaction.getType() == TransactionType.CREDIT ||
                transaction.getType() == TransactionType.TRANSFER_IN) {

                EntryRecord creditEntry = new EntryRecord();
                creditEntry.setAccountId(accountId);
                creditEntry.setTransactionId(UUID.fromString(transaction.getId()));
                creditEntry.setEntryType(EntryType.CREDIT);
                creditEntry.setAmount(transaction.getAmount().longValue() * 100); // Convert to cents
                creditEntry.setOperationDate(LocalDate.from(transaction.getTimestamp()));
                creditEntry.setIdempotencyKey(UUID.randomUUID()); // Should be from transaction
                creditEntry.setCurrencyCode("USD"); // Should be from transaction
                creditEntry.setCreatedAt(ZonedDateTime.now());

                entries.add(creditEntry);
            }

            // Сохраняем проводки в БД
            // В реальной реализации здесь был бы вызов repository
            log.debug("Created {} entry records for transaction {}", entries.size(), transaction.getId());

            return entries;
        });
    }

    private void triggerSnapshotIfNeeded(UUID accountId, LocalDate operationDate) {
        // Создаем снапшот асинхронно, если с последнего снапшота прошло достаточно времени
        // или накопилось достаточно операций
        snapshotService.requestSnapshot(accountId, operationDate)
            .whenComplete((result, throwable) -> {
                if (throwable == null && result.wasCreated()) {
                    log.debug("Auto-created snapshot for account {}: {} operations processed",
                        accountId, result.operationsProcessed());
                }
            });
    }

    // === API ENDPOINTS ===

    @GetMapping("/api/accounts/{accountId}/balance")
    public CompletableFuture<ResponseEntity<AccountBalance>> getBalance(
        @PathVariable UUID accountId,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate asOfDate) {

        LocalDate targetDate = asOfDate != null ? asOfDate : LocalDate.now();

        return balanceService.getBalance(accountId, targetDate)
            .thenApply(balance -> ResponseEntity.ok(balance))
            .exceptionally(throwable -> {
                log.error("Failed to get balance for account {}", accountId, throwable);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    @PostMapping("/api/accounts/{accountId}/snapshot")
    public CompletableFuture<ResponseEntity<SnapshotResult>> createSnapshot(
        @PathVariable UUID accountId,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate operationDate) {

        LocalDate targetDate = operationDate != null ? operationDate : LocalDate.now();

        return snapshotService.requestSnapshot(accountId, targetDate)
            .thenApply(result -> ResponseEntity.ok(result))
            .exceptionally(throwable -> {
                log.error("Failed to create snapshot for account {}", accountId, throwable);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    @GetMapping("/api/snapshots/statistics")
    public ResponseEntity<SnapshotStatistics> getSnapshotStatistics() {
        SnapshotStatistics stats = snapshotService.getStatistics();
        return ResponseEntity.ok(stats);
    }

    @GetMapping("/api/balance-service/statistics")
    public ResponseEntity<BalanceServiceStatistics> getBalanceServiceStatistics() {
        BalanceServiceStatistics stats = balanceService.getStatistics();
        return ResponseEntity.ok(stats);
    }
}

// ===== 8. КОНФИГУРАЦИЯ =====

@Configuration
public class SnapshotConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "ledger.snapshot")
    public SnapshotProperties snapshotProperties() {
        return new SnapshotProperties();
    }

    @ConfigurationProperties(prefix = "ledger.snapshot")
    public static class SnapshotProperties {
        private int batchSize = 100;
        private int maxConcurrent = 10;
        private int minOperationsThreshold = 50;
        private Duration cleanupRetention = Duration.ofDays(30);
        private boolean enablePeriodicSnapshots = true;

        // Getters and setters...
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

        public int getMaxConcurrent() { return maxConcurrent; }
        public void setMaxConcurrent(int maxConcurrent) { this.maxConcurrent = maxConcurrent; }

        public int getMinOperationsThreshold() { return minOperationsThreshold; }
        public void setMinOperationsThreshold(int minOperationsThreshold) {
            this.minOperationsThreshold = minOperationsThreshold;
        }

        public Duration getCleanupRetention() { return cleanupRetention; }
        public void setCleanupRetention(Duration cleanupRetention) {
            this.cleanupRetention = cleanupRetention;
        }

        public boolean isEnablePeriodicSnapshots() { return enablePeriodicSnapshots; }
        public void setEnablePeriodicSnapshots(boolean enablePeriodicSnapshots) {
            this.enablePeriodicSnapshots = enablePeriodicSnapshots;
        }
    }
}

// ===== 9. CLEANUP СЕРВИС =====

@Component
public class SnapshotCleanupService {

    private final SnapshotRepository snapshotRepository;
    private final SnapshotConfiguration.SnapshotProperties properties;

    public SnapshotCleanupService(SnapshotRepository snapshotRepository,
                                  SnapshotConfiguration.SnapshotProperties properties) {
        this.snapshotRepository = snapshotRepository;
        this.properties = properties;
    }

    @Scheduled(cron = "0 0 2 * * ?") // Каждый день в 2:00
    public void cleanupOldSnapshots() {
        ZonedDateTime cutoffDate = ZonedDateTime.now().minus(properties.getCleanupRetention());

        log.info("Starting cleanup of snapshots older than {}", cutoffDate);

        int batchSize = 1000;
        int totalDeleted = 0;

        while (true) {
            List<EntriesSnapshot> oldSnapshots = snapshotRepository
                .findOldSnapshotsForCleanup(cutoffDate, PageRequest.of(0, batchSize));

            if (oldSnapshots.isEmpty()) {
                break;
            }

            snapshotRepository.deleteAllInBatch(oldSnapshots);
            totalDeleted += oldSnapshots.size();

            log.debug("Deleted {} old snapshots (total: {})", oldSnapshots.size(), totalDeleted);

            // Пауза между батчами чтобы не нагружать БД
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        log.info("Snapshot cleanup completed: {} snapshots deleted", totalDeleted);
    }
}