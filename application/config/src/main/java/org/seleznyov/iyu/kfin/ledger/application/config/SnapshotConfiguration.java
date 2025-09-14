package org.seleznyov.iyu.kfin.ledger.application.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "ledger.snapshots.scheduled")
@Data
public class SnapshotConfiguration {

    /**
     * Enable/disable scheduled snapshots
     */
    private boolean enabled = true;

    /**
     * Cron expression for scheduling
     */
    private String cron = "0 0 2 * * *";

    /**
     * List of account IDs for scheduled snapshots
     */
    private List<String> accounts = new ArrayList<>();

    /**
     * Fallback strategy when no accounts configured:
     * - NONE: no snapshots
     * - ACTIVE: find active accounts from DB
     * - HOT: find hot accounts via ML detector
     */
    private FallbackStrategy fallbackStrategy = FallbackStrategy.NONE;

    /**
     * Maximum number of accounts to include in fallback strategies
     */
    private int maxFallbackAccounts = 100;

    /**
     * Days to look back for active accounts fallback
     */
    private int activeFallbackDays = 30;

    /**
     * Minimum activity level for hot accounts fallback
     */
    private double hotFallbackMinActivity = 0.5;

    public enum FallbackStrategy {
        NONE, ACTIVE, HOT
    }
}