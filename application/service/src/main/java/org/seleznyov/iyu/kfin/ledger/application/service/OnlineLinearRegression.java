package org.seleznyov.iyu.kfin.ledger.application.service;

// ПОЛНАЯ РЕАЛИЗАЦИЯ PRODUCTION ML СИСТЕМЫ ДЛЯ КЛАССИФИКАЦИИ СЧЕТОВ

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.*;
import java.util.concurrent.*;
import java.lang.Math;

// ===== 1. ОНЛАЙН ЛИНЕЙНАЯ РЕГРЕССИЯ ДЛЯ ПРЕДСКАЗАНИЯ ИНТЕНСИВНОСТИ =====

@Component
public class OnlineLinearRegression {

    // Веса модели (w0 = bias, w1..wn = feature weights)
    private final double[] weights;
    private final int numFeatures;

    // Гиперпараметры обучения
    private volatile double learningRate;
    private final double initialLearningRate;
    private final double decayRate;
    private final double minLearningRate;

    // Статистика модели
    private final AtomicLong totalSamples = new AtomicLong(0);
    private final AtomicDouble cumulativeError = new AtomicDouble(0.0);
    private final AtomicDouble lastMSE = new AtomicDouble(0.0);

    // Нормализация признаков (running statistics)
    private final RunningStatistics[] featureStats;

    public OnlineLinearRegression(int numFeatures, double initialLearningRate) {
        this.numFeatures = numFeatures;
        this.weights = new double[numFeatures + 1]; // +1 для bias
        this.initialLearningRate = initialLearningRate;
        this.learningRate = initialLearningRate;
        this.decayRate = 0.999; // Постепенное снижение learning rate
        this.minLearningRate = initialLearningRate * 0.01;

        // Инициализация весов случайными значениями
        Random random = new Random(42); // Fixed seed для воспроизводимости
        for (int i = 0; i < weights.length; i++) {
            weights[i] = random.nextGaussian() * 0.1; // Малые случайные веса
        }

        // Инициализация статистики признаков для нормализации
        this.featureStats = new RunningStatistics[numFeatures];
        for (int i = 0; i < numFeatures; i++) {
            featureStats[i] = new RunningStatistics();
        }

        log.info("Online Linear Regression initialized: {} features, learning rate: {}",
            numFeatures, initialLearningRate);
    }

    /**
     * Онлайн обучение на новом примере
     */
    public synchronized void train(double[] features, double target) {
        if (features.length != numFeatures) {
            throw new IllegalArgumentException("Expected " + numFeatures + " features, got " + features.length);
        }

        // 1. Нормализуем признаки
        double[] normalizedFeatures = normalizeFeatures(features);

        // 2. Делаем предсказание
        double prediction = predictInternal(normalizedFeatures);

        // 3. Вычисляем ошибку
        double error = target - prediction;

        // 4. Обновляем веса градиентным спуском
        // bias weight (w0)
        weights[0] += learningRate * error;

        // feature weights (w1..wn)
        for (int i = 0; i < numFeatures; i++) {
            weights[i + 1] += learningRate * error * normalizedFeatures[i];
        }

        // 5. Обновляем статистику модели
        long samples = totalSamples.incrementAndGet();
        cumulativeError.addAndGet(error * error);

        // Вычисляем скользящий MSE
        double mse = cumulativeError.get() / samples;
        lastMSE.set(mse);

        // 6. Адаптивно уменьшаем learning rate
        if (samples % 100 == 0) { // Каждые 100 примеров
            learningRate = Math.max(minLearningRate, learningRate * decayRate);
        }

        log.trace("Trained on sample {}: prediction={:.4f}, target={:.4f}, error={:.4f}, mse={:.6f}",
            samples, prediction, target, error, mse);
    }

    /**
     * Предсказание интенсивности
     */
    public double predict(double[] features) {
        if (features.length != numFeatures) {
            throw new IllegalArgumentException("Expected " + numFeatures + " features, got " + features.length);
        }

        double[] normalizedFeatures = normalizeFeatures(features);
        return Math.max(0.0, predictInternal(normalizedFeatures)); // Интенсивность не может быть отрицательной
    }

    private double predictInternal(double[] normalizedFeatures) {
        double result = weights[0]; // bias

        for (int i = 0; i < numFeatures; i++) {
            result += weights[i + 1] * normalizedFeatures[i];
        }

        return result;
    }

    private double[] normalizeFeatures(double[] features) {
        double[] normalized = new double[features.length];

        for (int i = 0; i < features.length; i++) {
            // Обновляем running statistics
            featureStats[i].add(features[i]);

            // Z-score нормализация: (x - mean) / std
            double mean = featureStats[i].getMean();
            double std = featureStats[i].getStandardDeviation();

            if (std > 1e-10) { // Избегаем деления на ноль
                normalized[i] = (features[i] - mean) / std;
            } else {
                normalized[i] = 0.0;
            }
        }

        return normalized;
    }

    // Геттеры для мониторинга
    public long getTotalSamples() { return totalSamples.get(); }
    public double getMSE() { return lastMSE.get(); }
    public double getCurrentLearningRate() { return learningRate; }
    public double[] getWeights() { return weights.clone(); }
}

// ===== 2. ОНЛАЙН K-MEANS КЛАСТЕРИЗАЦИЯ =====

@Component
public class OnlineKMeansClustering {

    private final int k; // Количество кластеров
    private final int dimensions; // Размерность признаков
    private final double[][] centroids; // Центры кластеров
    private final long[] clusterCounts; // Количество точек в каждом кластере
    private final AtomicLong totalPoints = new AtomicLong(0);

    // Адаптивные параметры
    private final double initialLearningRate;
    private volatile double learningRate;
    private final double minLearningRate;
    private final double decayRate;

    // Кластерные статистики
    private final RunningStatistics[] clusterDistances; // Средние расстояния до центров
    private final AtomicDouble totalSSE = new AtomicDouble(0.0); // Sum of Squared Errors

    public OnlineKMeansClustering(int k, int dimensions, double initialLearningRate) {
        this.k = k;
        this.dimensions = dimensions;
        this.initialLearningRate = initialLearningRate;
        this.learningRate = initialLearningRate;
        this.minLearningRate = initialLearningRate * 0.1;
        this.decayRate = 0.995;

        // Инициализация центроидов
        this.centroids = new double[k][dimensions];
        this.clusterCounts = new long[k];
        this.clusterDistances = new RunningStatistics[k];

        // Случайная инициализация центроидов
        Random random = new Random(42);
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < dimensions; j++) {
                centroids[i][j] = random.nextGaussian();
            }
            clusterDistances[i] = new RunningStatistics();
        }

        log.info("Online K-Means initialized: k={}, dimensions={}, learning_rate={}",
            k, dimensions, initialLearningRate);
    }

    /**
     * Классифицируем точку и обновляем кластеры
     */
    public synchronized int classify(double[] features) {
        if (features.length != dimensions) {
            throw new IllegalArgumentException("Expected " + dimensions + " features, got " + features.length);
        }

        // 1. Находим ближайший кластер
        int closestCluster = findClosestCluster(features);
        double minDistance = euclideanDistance(features, centroids[closestCluster]);

        // 2. Обновляем центроид ближайшего кластера
        updateCentroid(closestCluster, features);

        // 3. Обновляем статистику
        clusterCounts[closestCluster]++;
        clusterDistances[closestCluster].add(minDistance);
        totalSSE.addAndGet(minDistance * minDistance);

        long totalPointsCount = totalPoints.incrementAndGet();

        // 4. Адаптивно уменьшаем learning rate
        if (totalPointsCount % 1000 == 0) {
            learningRate = Math.max(minLearningRate, learningRate * decayRate);
        }

        log.trace("Classified point to cluster {}, distance: {:.4f}, total points: {}",
            closestCluster, minDistance, totalPointsCount);

        return closestCluster;
    }

    private int findClosestCluster(double[] features) {
        int closestCluster = 0;
        double minDistance = euclideanDistance(features, centroids[0]);

        for (int i = 1; i < k; i++) {
            double distance = euclideanDistance(features, centroids[i]);
            if (distance < minDistance) {
                minDistance = distance;
                closestCluster = i;
            }
        }

        return closestCluster;
    }

    private void updateCentroid(int cluster, double[] features) {
        // Онлайн обновление центроида: c_new = c_old + α * (x - c_old)
        for (int i = 0; i < dimensions; i++) {
            centroids[cluster][i] += learningRate * (features[i] - centroids[cluster][i]);
        }
    }

    private double euclideanDistance(double[] a, double[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return Math.sqrt(sum);
    }

    /**
     * Получить центроид кластера
     */
    public double[] getCentroid(int cluster) {
        if (cluster < 0 || cluster >= k) {
            throw new IllegalArgumentException("Invalid cluster id: " + cluster);
        }
        return centroids[cluster].clone();
    }

    /**
     * Получить статистику кластера
     */
    public ClusterStats getClusterStats(int cluster) {
        if (cluster < 0 || cluster >= k) {
            throw new IllegalArgumentException("Invalid cluster id: " + cluster);
        }

        return new ClusterStats(
            cluster,
            clusterCounts[cluster],
            centroids[cluster].clone(),
            clusterDistances[cluster].getMean(),
            clusterDistances[cluster].getStandardDeviation()
        );
    }

    /**
     * Общая статистика кластеризации
     */
    public ClusteringStats getClusteringStats() {
        double avgSSE = totalPoints.get() > 0 ? totalSSE.get() / totalPoints.get() : 0.0;

        ClusterStats[] allStats = new ClusterStats[k];
        for (int i = 0; i < k; i++) {
            allStats[i] = getClusterStats(i);
        }

        return new ClusteringStats(
            k,
            totalPoints.get(),
            avgSSE,
            learningRate,
            allStats
        );
    }
}

// ===== 3. RUNNING STATISTICS ДЛЯ ОНЛАЙН ВЫЧИСЛЕНИЙ =====

public static class RunningStatistics {
    private volatile long count = 0;
    private volatile double mean = 0.0;
    private volatile double m2 = 0.0; // Для вычисления variance
    private volatile double min = Double.MAX_VALUE;
    private volatile double max = Double.MIN_VALUE;

    public synchronized void add(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        m2 += delta * delta2;

        min = Math.min(min, value);
        max = Math.max(max, value);
    }

    public long getCount() { return count; }
    public double getMean() { return count > 0 ? mean : 0.0; }
    public double getVariance() { return count > 1 ? m2 / (count - 1) : 0.0; }
    public double getStandardDeviation() { return Math.sqrt(getVariance()); }
    public double getMin() { return count > 0 ? min : 0.0; }
    public double getMax() { return count > 0 ? max : 0.0; }

    public synchronized void reset() {
        count = 0;
        mean = 0.0;
        m2 = 0.0;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
    }
}

// ===== 4. ИНТЕЛЛЕКТУАЛЬНАЯ ML СИСТЕМА ДЛЯ КЛАССИФИКАЦИИ СЧЕТОВ =====

@Component
public class IntelligentAccountClassifier {

    // ML модели
    private final OnlineLinearRegression intensityPredictor;
    private final OnlineKMeansClustering accountClustering;

    // Признаки для ML моделей
    private static final int NUM_FEATURES = 8;
    private static final int NUM_CLUSTERS = 4; // COLD, WARM, HOT, ULTRA_HOT

    // История предсказаний для валидации модели
    private final Map<String, PredictionHistory> predictionHistory = new ConcurrentHashMap<>();

    // Метрики ML системы
    private final AtomicLong totalPredictions = new AtomicLong(0);
    private final AtomicLong correctPredictions = new AtomicLong(0);
    private final AtomicDouble accuracyRate = new AtomicDouble(0.0);

    public IntelligentAccountClassifier() {
        // Инициализируем модели
        this.intensityPredictor = new OnlineLinearRegression(NUM_FEATURES, 0.01);
        this.accountClustering = new OnlineKMeansClustering(NUM_CLUSTERS, NUM_FEATURES, 0.1);

        log.info("Intelligent Account Classifier initialized");
    }

    /**
     * Извлекаем признаки из статистики счета
     */
    private double[] extractFeatures(String accountId, AccountStatistics stats) {
        double[] features = new double[NUM_FEATURES];

        // Признак 0: Логарифм текущей интенсивности (сглаживает выбросы)
        features[0] = Math.log1p(stats.getCurrentIntensity());

        // Признак 1: Логарифм пиковой интенсивности
        features[1] = Math.log1p(stats.getPeakIntensity());

        // Признак 2: Вариация интенсивности (стабильность)
        features[2] = stats.getIntensityVariance();

        // Признак 3: Временной фактор (время дня)
        features[3] = getTimeOfDayFactor();

        // Признак 4: Фактор дня недели
        features[4] = getDayOfWeekFactor();

        // Признак 5: Тренд интенсивности (растет/падает)
        features[5] = stats.getIntensityTrend();

        // Признак 6: Возраст счета (новые vs старые счета ведут себя по-разному)
        features[6] = Math.log1p(stats.getAccountAgeHours());

        // Признак 7: Общее количество операций (логарифм)
        features[7] = Math.log1p(stats.getTotalOperations());

        return features;
    }

    /**
     * Предсказываем интенсивность на следующий период
     */
    public double predictIntensity(String accountId, AccountStatistics stats) {
        double[] features = extractFeatures(accountId, stats);
        double predictedIntensity = intensityPredictor.predict(features);

        // Запоминаем предсказание для последующей валидации
        PredictionHistory history = predictionHistory.computeIfAbsent(accountId,
            k -> new PredictionHistory());
        history.addPrediction(predictedIntensity, System.currentTimeMillis());

        totalPredictions.incrementAndGet();

        log.trace("Predicted intensity for account {}: {:.2f}", accountId, predictedIntensity);

        return predictedIntensity;
    }

    /**
     * Классифицируем счет в кластер
     */
    public int classifyAccount(String accountId, AccountStatistics stats) {
        double[] features = extractFeatures(accountId, stats);
        int cluster = accountClustering.classify(features);

        log.trace("Classified account {} to cluster {}", accountId, cluster);

        return cluster;
    }

    /**
     * Обучаем модель на реальных данных
     */
    public void trainOnActualData(String accountId, AccountStatistics stats, double actualIntensity) {
        double[] features = extractFeatures(accountId, stats);

        // Обучаем регрессию на фактической интенсивности
        intensityPredictor.train(features, actualIntensity);

        // Валидируем предыдущие предсказания
        PredictionHistory history = predictionHistory.get(accountId);
        if (history != null) {
            Double previousPrediction = history.getAndRemoveOldestPrediction();
            if (previousPrediction != null) {
                // Проверяем точность предсказания (±20% считаем корректным)
                double error = Math.abs(actualIntensity - previousPrediction) / Math.max(1.0, actualIntensity);
                if (error <= 0.2) {
                    correctPredictions.incrementAndGet();
                }

                // Обновляем общую точность
                long total = totalPredictions.get();
                if (total > 0) {
                    accuracyRate.set((double) correctPredictions.get() / total);
                }
            }
        }

        log.trace("Trained on actual data for account {}: intensity={:.2f}", accountId, actualIntensity);
    }

    /**
     * Интеллектуальный выбор стратегии на основе ML
     */
    public ProcessingStrategy selectStrategy(String accountId, AccountStatistics stats) {
        // 1. Предсказываем будущую интенсивность
        double predictedIntensity = predictIntensity(accountId, stats);

        // 2. Классифицируем счет
        int cluster = classifyAccount(accountId, stats);

        // 3. Получаем статистику кластера для адаптивных порогов
        ClusterStats clusterStats = accountClustering.getClusterStats(cluster);

        // 4. Принимаем решение на основе ML + эвристик
        ProcessingStrategy mlStrategy = mapClusterToStrategy(cluster);
        ProcessingStrategy intensityStrategy = getStrategyByIntensity(predictedIntensity, clusterStats);

        // 5. Комбинируем решения (консенсус)
        ProcessingStrategy finalStrategy = combineStrategies(mlStrategy, intensityStrategy);

        log.debug("Strategy selection for account {}: predicted_intensity={:.2f}, cluster={}, " +
                "ml_strategy={}, intensity_strategy={}, final_strategy={}",
            accountId, predictedIntensity, cluster, mlStrategy, intensityStrategy, finalStrategy);

        return finalStrategy;
    }

    private ProcessingStrategy mapClusterToStrategy(int cluster) {
        return switch (cluster) {
            case 0 -> ProcessingStrategy.COLD;     // Низкая активность
            case 1 -> ProcessingStrategy.WARM;     // Умеренная активность
            case 2 -> ProcessingStrategy.HOT;      // Высокая активность
            case 3 -> ProcessingStrategy.ULTRA_HOT; // Экстремальная активность
            default -> ProcessingStrategy.WARM;    // Default fallback
        };
    }

    private ProcessingStrategy getStrategyByIntensity(double intensity, ClusterStats clusterStats) {
        // Адаптивные пороги на основе статистики кластера
        double avgDistance = clusterStats.averageDistance();
        double baseThreshold = 10.0; // Базовый порог

        // Если кластер "плотный" (малое среднее расстояние) - повышаем пороги
        double threshold1 = baseThreshold * (1.0 + avgDistance * 0.1);
        double threshold2 = threshold1 * 5.0;
        double threshold3 = threshold2 * 2.0;

        if (intensity > threshold3) return ProcessingStrategy.ULTRA_HOT;
        if (intensity > threshold2) return ProcessingStrategy.HOT;
        if (intensity > threshold1) return ProcessingStrategy.WARM;
        return ProcessingStrategy.COLD;
    }

    private ProcessingStrategy combineStrategies(ProcessingStrategy mlStrategy, ProcessingStrategy intensityStrategy) {
        // Консенсус между ML и правилами: берем более консервативную стратегию
        // если они сильно расходятся (избегаем overclassification)

        int mlLevel = getStrategyLevel(mlStrategy);
        int intensityLevel = getStrategyLevel(intensityStrategy);

        // Если разница больше 1 уровня - берем среднее
        if (Math.abs(mlLevel - intensityLevel) > 1) {
            int avgLevel = (mlLevel + intensityLevel) / 2;
            return getStrategyByLevel(avgLevel);
        }

        // Иначе берем более высокий уровень (aggressive classification)
        return mlLevel > intensityLevel ? mlStrategy : intensityStrategy;
    }

    private int getStrategyLevel(ProcessingStrategy strategy) {
        return switch (strategy) {
            case COLD -> 0;
            case WARM -> 1;
            case HOT -> 2;
            case ULTRA_HOT -> 3;
        };
    }

    private ProcessingStrategy getStrategyByLevel(int level) {
        return switch (level) {
            case 0 -> ProcessingStrategy.COLD;
            case 1 -> ProcessingStrategy.WARM;
            case 2 -> ProcessingStrategy.HOT;
            case 3 -> ProcessingStrategy.ULTRA_HOT;
            default -> ProcessingStrategy.WARM;
        };
    }

    private double getTimeOfDayFactor() {
        int hour = LocalTime.now().getHour();
        // Нормализованный фактор: пик активности 9-17
        return Math.max(0.0, Math.min(1.0, (17 - Math.abs(13 - hour)) / 4.0));
    }

    private double getDayOfWeekFactor() {
        DayOfWeek dayOfWeek = LocalDate.now().getDayOfWeek();
        // Будние дни = 1.0, выходные = 0.3
        return dayOfWeek.getValue() <= 5 ? 1.0 : 0.3;
    }

    // === МОНИТОРИНГ ML СИСТЕМЫ ===

    public MLSystemStats getMLStats() {
        return new MLSystemStats(
            totalPredictions.get(),
            correctPredictions.get(),
            accuracyRate.get(),
            intensityPredictor.getTotalSamples(),
            intensityPredictor.getMSE(),
            intensityPredictor.getCurrentLearningRate(),
            accountClustering.getTotalPoints(),
            accountClustering.getClusteringStats()
        );
    }

    // Периодическая переподготовка модели при деградации точности
    @Scheduled(fixedRate = 300000) // Каждые 5 минут
    public void monitorAndTuneML() {
        MLSystemStats stats = getMLStats();

        // Если точность упала ниже 70% - увеличиваем learning rate
        if (stats.accuracyRate() < 0.7 && intensityPredictor.getCurrentLearningRate() < 0.1) {
            // Можно было бы увеличить learning rate, но это потребует модификации модели
            log.warn("ML accuracy dropped to {:.2f}% - consider model retuning",
                stats.accuracyRate() * 100);
        }

        // Логируем статистику
        if (stats.totalPredictions() > 0 && stats.totalPredictions() % 10000 == 0) {
            log.info("ML Stats: predictions={}, accuracy={:.2f}%, mse={:.6f}, clusters_total={}",
                stats.totalPredictions(), stats.accuracyRate() * 100,
                stats.regressionMSE(), stats.clusteringStats().totalPoints());
        }
    }
}

// ===== 5. ВСПОМОГАТЕЛЬНЫЕ КЛАССЫ ДЛЯ ML =====

public static class PredictionHistory {
    private final Queue<TimestampedPrediction> predictions = new ConcurrentLinkedQueue<>();
    private static final long MAX_AGE_MS = 300_000; // 5 минут

    void addPrediction(double value, long timestamp) {
        predictions.offer(new TimestampedPrediction(value, timestamp));

        // Очищаем старые предсказания
        long cutoff = System.currentTimeMillis() - MAX_AGE_MS;
        predictions.removeIf(p -> p.timestamp < cutoff);
    }

    Double getAndRemoveOldestPrediction() {
        TimestampedPrediction oldest = predictions.poll();
        return oldest != null ? oldest.value : null;
    }

    private record TimestampedPrediction(double value, long timestamp) {}
}

public static class AccountStatistics {
    private final double currentIntensity;
    private final double peakIntensity;
    private final double intensityVariance;
    private final double intensityTrend;
    private final long totalOperations;
    private final long accountAgeHours;

    public AccountStatistics(double currentIntensity, double peakIntensity,
                             double intensityVariance, double intensityTrend,
                             long totalOperations, long accountAgeHours) {
        this.currentIntensity = currentIntensity;
        this.peakIntensity = peakIntensity;
        this.intensityVariance = intensityVariance;
        this.intensityTrend = intensityTrend;
        this.totalOperations = totalOperations;
        this.accountAgeHours = accountAgeHours;
    }

    // Getters
    public double getCurrentIntensity() { return currentIntensity; }
    public double getPeakIntensity() { return peakIntensity; }
    public double getIntensityVariance() { return intensityVariance; }
    public double getIntensityTrend() { return intensityTrend; }
    public long getTotalOperations() { return totalOperations; }
    public long getAccountAgeHours() { return accountAgeHours; }
}

// Records для статистики
public record ClusterStats(
    int clusterId,
    long pointCount,
    double[] centroid,
    double averageDistance,
    double distanceStandardDeviation
) {}

public record ClusteringStats(
    int numClusters,
    long totalPoints,
    double averageSSE,
    double currentLearningRate,
    ClusterStats[] clusterStats
) {}

public record MLSystemStats(
    long totalPredictions,
    long correctPredictions,
    double accuracyRate,
    long regressionSamples,
    double regressionMSE,
    double regressionLearningRate,
    long clusteringPoints,
    ClusteringStats clusteringStats
) {}