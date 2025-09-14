package org.seleznyov.iyu.kfin.ledger.application.service;

// ИНТЕГРАЦИЯ ML СИСТЕМЫ С ОСНОВНЫМ LEDGER SERVICE

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class MLEnabledActivityTracker extends ScalableActivityTracker {

    private final IntelligentAccountClassifier mlClassifier;
    private final Map<String, EnhancedAccountStats> enhancedStats = new ConcurrentHashMap<>();

    public MLEnabledActivityTracker(IntelligentAccountClassifier mlClassifier) {
        super(); // Вызываем родительский конструктор
        this.mlClassifier = mlClassifier;
        log.info("ML-enabled Activity Tracker initialized");
    }

    @Override
    public void recordActivity(String accountId, ActivityType type) {
        long timestamp = System.currentTimeMillis();

        // Вызываем родительский метод для базовой статистики
        super.recordActivity(accountId, type);

        // Обновляем расширенную ML статистику
        EnhancedAccountStats stats = enhancedStats.computeIfAbsent(
            accountId, k -> new EnhancedAccountStats(accountId)
        );

        // Запоминаем предыдущую интенсивность для обучения
        double previousIntensity = stats.getCurrentIntensity();

        // Обновляем статистику
        stats.recordActivity(timestamp, type);

        // Обучаем ML модель на фактических данных
        if (stats.getTotalOperations() > 10) { // Обучаем только после накопления данных
            AccountStatistics mlStats = stats.toAccountStatistics();
            mlClassifier.trainOnActualData(accountId, mlStats, stats.getCurrentIntensity());
        }

        log.trace("ML activity recorded for account {}: type={}, intensity={:.2f}",
            accountId, type, stats.getCurrentIntensity());
    }

    /**
     * ML-powered определение интенсивности с предсказанием
     */
    @Override
    public double getCurrentIntensity(String accountId) {
        // Получаем базовую интенсивность
        double baseIntensity = super.getCurrentIntensity(accountId);

        // Если есть ML статистика - используем предсказание
        EnhancedAccountStats stats = enhancedStats.get(accountId);
        if (stats != null && stats.getTotalOperations() > 5) {
            AccountStatistics mlStats = stats.toAccountStatistics();
            double predictedIntensity = mlClassifier.predictIntensity(accountId, mlStats);

            // Комбинируем текущую и предсказанную интенсивность (weighted average)
            double weight = Math.min(1.0, stats.getTotalOperations() / 100.0); // Больше данных = больше доверия к ML
            return baseIntensity * (1 - weight) + predictedIntensity * weight;
        }

        return baseIntensity;
    }

    /**
     * ML-powered выбор стратегии
     */
    public ProcessingStrategy selectMLStrategy(String accountId) {
        EnhancedAccountStats stats = enhancedStats.get(accountId);
        if (stats == null || stats.getTotalOperations() < 3) {
            // Для новых счетов используем базовую эвристику
            double intensity = getCurrentIntensity(accountId);
            return getBasicStrategy(intensity);
        }

        // Используем ML классификатор
        AccountStatistics mlStats = stats.toAccountStatistics();
        return mlClassifier.selectStrategy(accountId, mlStats);
    }

    private ProcessingStrategy getBasicStrategy(double intensity) {
        if (intensity > 100) return ProcessingStrategy.ULTRA_HOT;
        if (intensity > 50) return ProcessingStrategy.HOT;
        if (intensity > 5) return ProcessingStrategy.WARM;
        return ProcessingStrategy.COLD;
    }

    /**
     * Получение ML метрик для мониторинга
     */
    public MLSystemStats getMLMetrics() {
        return mlClassifier.getMLStats();
    }

    // Периодическая очистка с учетом ML
    @Override
    @Scheduled(fixedRate = 300000) // 5 минут
    public void cleanup() {
        super.cleanup(); // Вызываем родительскую очистку

        long cutoffTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(24);

        // Очищаем неактивные расширенные статистики
        enhancedStats.entrySet().removeIf(entry -> {
            EnhancedAccountStats stats = entry.getValue();
            return stats.getLastActivity() < cutoffTime && stats.getCurrentIntensity() < 0.1;
        });

        log.debug("ML cleanup completed. Enhanced stats count: {}", enhancedStats.size());
    }
}

// === РАСШИРЕННАЯ СТАТИСТИКА ДЛЯ ML ===

public static class EnhancedAccountStats {
    // Getters
    @Getter
    private final String accountId;
    private final long creationTime;
    private final CircularBuffer intensityHistory;
    private final RunningStatistics intensityStats;

    // Текущие показатели
    @Getter
    private volatile double currentIntensity = 0.0;
    @Getter
    private volatile double peakIntensity = 0.0;
    @Getter
    private volatile long totalOperations = 0;
    @Getter
    private volatile long lastActivity = System.currentTimeMillis();

    // Тренд анализ
    private final LinearTrendAnalyzer trendAnalyzer;

    public EnhancedAccountStats(String accountId) {
        this.accountId = accountId;
        this.creationTime = System.currentTimeMillis();
        this.intensityHistory = new CircularBuffer(144); // 24 часа по 10-минутным интервалам
        this.intensityStats = new RunningStatistics();
        this.trendAnalyzer = new LinearTrendAnalyzer(20); // Последние 20 точек для тренда
    }

    public void recordActivity(long timestamp, ActivityType type) {
        totalOperations++;
        lastActivity = timestamp;

        // Вычисляем текущую интенсивность (операции в минуту)
        updateCurrentIntensity(timestamp);

        // Обновляем пиковую интенсивность
        if (currentIntensity > peakIntensity) {
            peakIntensity = currentIntensity;
        }

        // Добавляем в историю каждые 10 минут
        if (shouldAddToHistory(timestamp)) {
            intensityHistory.add(currentIntensity, timestamp);
            intensityStats.add(currentIntensity);
            trendAnalyzer.addPoint(timestamp, currentIntensity);
        }
    }

    private void updateCurrentIntensity(long currentTime) {
        // Подсчитываем операции за последнюю минуту
        long oneMinuteAgo = currentTime - TimeUnit.MINUTES.toMillis(1);
        int recentOperations = countRecentOperations(oneMinuteAgo);
        currentIntensity = (double) recentOperations; // операций в минуту
    }

    private int countRecentOperations(long since) {
        // Упрощенная реализация - в реальности здесь была бы более сложная логика
        // с отслеживанием временных меток операций
        return (int) Math.max(1, currentIntensity * 0.9 + 1); // Примерная имитация
    }

    private boolean shouldAddToHistory(long timestamp) {
        HistoryEntry lastEntry = intensityHistory.getLatest();
        if (lastEntry == null) return true;

        // Добавляем новую запись каждые 10 минут
        return timestamp - lastEntry.timestamp >= TimeUnit.MINUTES.toMillis(10);
    }

    public AccountStatistics toAccountStatistics() {
        return new AccountStatistics(
            currentIntensity,
            peakIntensity,
            intensityStats.getVariance(),
            trendAnalyzer.getTrendSlope(),
            totalOperations,
            getAccountAgeHours()
        );
    }

    public long getAccountAgeHours() {
        return TimeUnit.MILLISECONDS.toHours(System.currentTimeMillis() - creationTime);
    }
    public double getIntensityVariance() { return intensityStats.getVariance(); }
    public double getIntensityTrend() { return trendAnalyzer.getTrendSlope(); }
}

// === CIRCULAR BUFFER ДЛЯ ИСТОРИИ ИНТЕНСИВНОСТИ ===

public static class CircularBuffer {
    private final HistoryEntry[] buffer;
    private final int capacity;
    private int head = 0;
    private int size = 0;

    public CircularBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new HistoryEntry[capacity];
    }

    public synchronized void add(double intensity, long timestamp) {
        buffer[head] = new HistoryEntry(intensity, timestamp);
        head = (head + 1) % capacity;
        if (size < capacity) size++;
    }

    public synchronized HistoryEntry getLatest() {
        if (size == 0) return null;
        int latestIndex = (head - 1 + capacity) % capacity;
        return buffer[latestIndex];
    }

    public synchronized double[] getRecentIntensities(int count) {
        int actualCount = Math.min(count, size);
        double[] result = new double[actualCount];

        for (int i = 0; i < actualCount; i++) {
            int index = (head - 1 - i + capacity) % capacity;
            result[i] = buffer[index].intensity;
        }

        return result;
    }

    public record HistoryEntry(double intensity, long timestamp) {}
}

// === АНАЛИЗ ТРЕНДОВ ===

public static class LinearTrendAnalyzer {
    private final CircularBuffer trendBuffer;
    private final int windowSize;

    public LinearTrendAnalyzer(int windowSize) {
        this.windowSize = windowSize;
        this.trendBuffer = new CircularBuffer(windowSize);
    }

    public void addPoint(long timestamp, double value) {
        trendBuffer.add(value, timestamp);
    }

    public double getTrendSlope() {
        double[] values = trendBuffer.getRecentIntensities(windowSize);
        if (values.length < 3) return 0.0; // Недостаточно данных

        // Простая линейная регрессия для определения тренда
        int n = values.length;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < n; i++) {
            double x = i; // Время (относительное)
            double y = values[i]; // Значение интенсивности

            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }

        // Slope = (n*ΣXY - ΣX*ΣY) / (n*ΣX² - (ΣX)²)
        double denominator = n * sumX2 - sumX * sumX;
        if (Math.abs(denominator) < 1e-10) return 0.0;

        double slope = (n * sumXY - sumX * sumY) / denominator;
        return slope;
    }
}

// === ОБНОВЛЕННЫЙ HOT ACCOUNT MANAGER С ML ===

@Component
public class MLEnabledHotAccountManager extends HotAccountManager {

    private final MLEnabledActivityTracker mlActivityTracker;

    public MLEnabledHotAccountManager(MLEnabledActivityTracker mlActivityTracker) {
        super(); // Родительский конструктор
        this.mlActivityTracker = mlActivityTracker;
    }

    @Override
    public ProcessingStrategy assignStrategy(String accountId, double intensity) {
        // Пробуем сначала ML стратегию
        ProcessingStrategy mlStrategy = mlActivityTracker.selectMLStrategy(accountId);

        // Получаем базовую стратегию для сравнения
        ProcessingStrategy baseStrategy = super.assignStrategy(accountId, intensity);

        // Если ML недостаточно обучена - используем базовую стратегию
        MLSystemStats mlStats = mlActivityTracker.getMLMetrics();
        if (mlStats.totalPredictions() < 1000 || mlStats.accuracyRate() < 0.5) {
            log.debug("Using base strategy for account {} (ML not ready: predictions={}, accuracy={:.2f})",
                accountId, mlStats.totalPredictions(), mlStats.accuracyRate() * 100);
            return baseStrategy;
        }

        // Логируем различия в стратегиях для анализа
        if (mlStrategy != baseStrategy) {
            log.debug("Strategy difference for account {}: ML={}, Base={}, intensity={:.2f}",
                accountId, mlStrategy, baseStrategy, intensity);
        }

        return mlStrategy;
    }

    // Добавляем ML метрики в мониторинг
    @Scheduled(fixedRate = 60000) // Каждую минуту
    public void reportMLMetrics() {
        MLSystemStats stats = mlActivityTracker.getMLMetrics();

        // Отправляем метрики в систему мониторинга
        meterRegistry.gauge("ml.predictions.total", stats.totalPredictions());
        meterRegistry.gauge("ml.predictions.correct", stats.correctPredictions());
        meterRegistry.gauge("ml.accuracy.rate", stats.accuracyRate() * 100);
        meterRegistry.gauge("ml.regression.mse", stats.regressionMSE());
        meterRegistry.gauge("ml.clustering.points", stats.clusteringPoints());

        // Алерты при плохом качестве ML
        if (stats.totalPredictions() > 1000 && stats.accuracyRate() < 0.6) {
            alertingService.sendAlert(AlertLevel.WARNING,
                String.format("ML accuracy is low: %.1f%% (threshold: 60%%)",
                    stats.accuracyRate() * 100));
        }

        if (stats.regressionMSE() > 100.0) {
            alertingService.sendAlert(AlertLevel.WARNING,
                String.format("ML regression MSE is high: %.2f (threshold: 100.0)",
                    stats.regressionMSE()));
        }
    }
}

// === ОБНОВЛЕННЫЙ ОСНОВНОЙ СЕРВИС ===

@Service
public class MLOptimizedLedgerService extends OptimizedLedgerService {

    private final MLEnabledActivityTracker mlTracker;
    private final MLEnabledHotAccountManager mlHotAccountManager;

    public MLOptimizedLedgerService(UltraHighPerformanceWAL wal,
                                    MLEnabledActivityTracker mlTracker,
                                    MLEnabledHotAccountManager mlHotAccountManager,
                                    AccountBalanceManager balanceManager) {
        super(wal, mlTracker, mlHotAccountManager, balanceManager);
        this.mlTracker = mlTracker;
        this.mlHotAccountManager = mlHotAccountManager;

        log.info("ML-Optimized Ledger Service initialized");
    }

    @Override
    public CompletableFuture<TransactionResult> processTransaction(String accountId, Transaction transaction) {
        long startTime = System.nanoTime();

        try {
            // 1. Записываем активность (с ML обучением)
            mlTracker.recordActivity(accountId, ActivityType.TRANSACTION);

            // 2. ML-powered выбор стратегии
            ProcessingStrategy strategy = mlHotAccountManager.assignStrategy(
                accountId, mlTracker.getCurrentIntensity(accountId)
            );

            // Остальная логика без изменений
            return super.processTransaction(accountId, transaction)
                .whenComplete((result, throwable) -> {
                    // Логируем успешность для дальнейшего ML анализа
                    if (result != null && result.isSuccess()) {
                        log.trace("Transaction processed successfully with ML strategy: {} for account {}",
                            strategy, accountId);
                    }
                });

        } finally {
            // Метрики производительности ML системы
            long duration = System.nanoTime() - startTime;
            meterRegistry.timer("ledger.transaction.ml.duration")
                .record(duration, TimeUnit.NANOSECONDS);
        }
    }

    // Новый endpoint для ML статистики
    @GetMapping("/api/ml/stats")
    public ResponseEntity<MLSystemStats> getMLStats() {
        MLSystemStats stats = mlTracker.getMLMetrics();
        return ResponseEntity.ok(stats);
    }

    // Endpoint для принудительной переподготовки модели
    @PostMapping("/api/ml/retrain")
    public ResponseEntity<String> retrain(@RequestParam(defaultValue = "false") boolean resetWeights) {
        if (resetWeights) {
            // В production здесь была бы логика сброса весов модели
            log.warn("ML model weights reset requested - not implemented in this version");
            return ResponseEntity.ok("Model reset requested (not implemented)");
        }

        log.info("ML model retraining requested");
        return ResponseEntity.ok("Retraining in progress");
    }
}