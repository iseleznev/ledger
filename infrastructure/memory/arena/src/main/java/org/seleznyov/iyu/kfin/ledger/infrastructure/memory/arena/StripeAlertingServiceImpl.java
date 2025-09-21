package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

// Реализация
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.scheduling.annotation.Async;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Service
public class StripeAlertingServiceImpl implements StripeAlertingService {
    private static final Logger log = LoggerFactory.getLogger(StripeAlertingServiceImpl.class);

    private final RestTemplate restTemplate;
    private final NotificationChannels channels;
    private final StripeAlertThrottler throttler;

    @Value("${alerting.enabled:true}")
    private boolean alertingEnabled;

    @Value("${alerting.environment:dev}")
    private String environment;

    public StripeAlertingServiceImpl(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        this.channels = new NotificationChannels();
        this.throttler = new StripeAlertThrottler();
    }

    @Override
    @Async("alertExecutor") // Асинхронная отправка алертов
    public void sendAlert(AlertLevel level, String message, Map<String, Object> details) {
        if (!alertingEnabled) {
            log.debug("Alerting disabled, skipping alert: {}", message);
            return;
        }

        String alertKey = generateAlertKey(message, details);

        // Throttling - не спамим одинаковыми алертами
        if (throttler.shouldThrottle(alertKey, level)) {
            log.debug("Alert throttled: {}", alertKey);
            return;
        }

        try {
            AlertPayload payload = AlertPayload.builder()
                .level(level)
                .message(message)
                .details(details)
                .environment(environment)
                .timestamp(Instant.now())
                .alertKey(alertKey)
                .build();

            // Отправляем в разные каналы в зависимости от уровня
            sendToChannels(payload);

            log.info("Alert sent: {} - {}", level, message);

        } catch (Exception e) {
            log.error("Failed to send alert: {} - {}", message, e.getMessage(), e);
        }
    }

    @Override
    public void sendCriticalAlert(String title, String message, Map<String, Object> details) {
        // Критические алерты всегда отправляем, даже если throttling
        Map<String, Object> criticalDetails = new HashMap<>(details);
        criticalDetails.put("title", title);
        criticalDetails.put("bypass_throttling", true);

        sendAlert(AlertLevel.CRITICAL, message, criticalDetails);

        // Дополнительно - отправляем в PagerDuty или другие критические каналы
        sendToPagerDuty(title, message, criticalDetails);
    }

    private void sendToChannels(AlertPayload payload) {
        AlertLevel level = payload.getLevel();

        // Slack для всех алертов
        if (level.getPriority() >= AlertLevel.MEDIUM.getPriority()) {
            channels.sendToSlack(payload);
        }

        // Email для высоких уровней
        if (level.getPriority() >= AlertLevel.HIGH.getPriority()) {
            channels.sendToEmail(payload);
        }

        // SMS/Phone для критических
        if (level == AlertLevel.CRITICAL) {
            channels.sendToSms(payload);
        }

        // Telegram для разработчиков
        if (environment.equals("dev")) {
            channels.sendToTelegram(payload);
        }
    }

    private void sendToPagerDuty(String title, String message, Map<String, Object> details) {
        // Реализация для PagerDuty
        try {
            PagerDutyEvent event = PagerDutyEvent.builder()
                .summary(title)
                .severity("critical")
                .source("ledger-service")
                .component("stripe-workers")
                .customDetails(details)
                .build();

            // Отправка в PagerDuty API
            log.info("Critical alert sent to PagerDuty: {}", title);
        } catch (Exception e) {
            log.error("Failed to send to PagerDuty", e);
        }
    }

    private String generateAlertKey(String message, Map<String, Object> details) {
        // Генерируем ключ для throttling на основе сообщения и ключевых деталей
        String key = message;
        if (details.containsKey("workerId")) {
            key += "_worker_" + details.get("workerId");
        }
        return key.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    @Override
    public boolean isAlertingEnabled() {
        return alertingEnabled;
    }

    @Override
    public void setAlertingEnabled(boolean enabled) {
        this.alertingEnabled = enabled;
        log.info("Alerting {}", enabled ? "enabled" : "disabled");
    }
}