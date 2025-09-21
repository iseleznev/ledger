package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import java.util.Map;

// Интерфейс
public interface StripeAlertingService {
    void sendAlert(AlertLevel level, String message, Map<String, Object> details);
    void sendCriticalAlert(String title, String message, Map<String, Object> details);
    boolean isAlertingEnabled();
    void setAlertingEnabled(boolean enabled);
}