package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena;

import lombok.Builder;
import lombok.Data;
import java.time.Instant;
import java.util.Map;

@Data
@Builder
public class AlertPayload {
    private AlertLevel level;
    private String message;
    private String environment;
    private Instant timestamp;
    private String alertKey;
    private Map<String, Object> details;
}

// Для Slack
@Data
@Builder
public class SlackMessage {
    private String text;
    private String username;
    @JsonProperty("icon_emoji")
    private String iconEmoji;
}