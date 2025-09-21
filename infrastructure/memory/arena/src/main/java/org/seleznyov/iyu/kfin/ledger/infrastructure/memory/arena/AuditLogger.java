package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Map;

@Slf4j
public class AuditLogger {
//    private static final Logger auditLog = LoggerFactory.getLogger("AUDIT");

    public void logSecurityEvent(String eventType, Map<String, Object> details) {
        try {
            AuditEvent event = AuditEvent.builder()
                .eventType(eventType)
                .timestamp(Instant.now())
                .details(details)
                .build();

            // Structured logging
            auditLog.info("AUDIT_EVENT: {}", objectMapper.writeValueAsString(event));

            // Опционально - отправка в audit систему
            sendToAuditSystem(event);

        } catch (Exception e) {
            log.error("Failed to log audit event", e);
        }
    }

    private void sendToAuditSystem(AuditEvent event) {
        // Отправка в централизованную audit систему
        // (Elasticsearch, Splunk, etc.)
    }

    @Data
    @Builder
    public static class AuditEvent {
        private String eventType;
        private Instant timestamp;
        private Map<String, Object> details;
    }
}