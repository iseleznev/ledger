package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StripeAlertThrottler {
    private final ConcurrentHashMap<String, ThrottleInfo> throttleMap = new ConcurrentHashMap<>();

    // Настройки throttling для разных уровней
    private static final Duration LOW_THROTTLE = Duration.ofMinutes(30);
    private static final Duration MEDIUM_THROTTLE = Duration.ofMinutes(15);
    private static final Duration HIGH_THROTTLE = Duration.ofMinutes(5);
    private static final Duration CRITICAL_THROTTLE = Duration.ofMinutes(2);

    public boolean shouldThrottle(String alertKey, AlertLevel level) {
        ThrottleInfo info = throttleMap.get(alertKey);
        Instant now = Instant.now();

        if (info == null) {
            // Первый раз - не throttling
            throttleMap.put(alertKey, new ThrottleInfo(now, 1));
            return false;
        }

        Duration throttlePeriod = getThrottlePeriod(level);

        if (now.isBefore(info.lastSent.plus(throttlePeriod))) {
            // Еще не прошло достаточно времени
            info.incrementCount();
            return true;
        }

        // Время прошло - можно отправлять
        info.reset(now);
        return false;
    }

    private Duration getThrottlePeriod(AlertLevel level) {
        return switch (level) {
            case LOW -> LOW_THROTTLE;
            case MEDIUM -> MEDIUM_THROTTLE;
            case HIGH -> HIGH_THROTTLE;
            case CRITICAL -> CRITICAL_THROTTLE;
        };
    }

    @Scheduled(fixedRate = 3600000) // Очистка каждый час
    public void cleanup() {
        Instant cutoff = Instant.now().minus(Duration.ofHours(2));
        throttleMap.entrySet().removeIf(entry ->
            entry.getValue().lastSent.isBefore(cutoff));
    }

    private static class ThrottleInfo {
        Instant lastSent;
        int count;

        ThrottleInfo(Instant lastSent, int count) {
            this.lastSent = lastSent;
            this.count = count;
        }

        void incrementCount() {
            count++;
        }

        void reset(Instant newTime) {
            lastSent = newTime;
            count = 1;
        }
    }
}