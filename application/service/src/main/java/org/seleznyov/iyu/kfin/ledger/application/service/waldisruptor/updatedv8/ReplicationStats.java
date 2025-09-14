package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updatedv8;

/**
 * Статистика репликации
 */
public record ReplicationStats(
    long totalAttempts,
    long totalFailures,
    int secondaryNodeCount,
    int backupTargetCount
) {

    public double getSuccessRate() {
        return totalAttempts > 0 ? (double) (totalAttempts - totalFailures) / totalAttempts * 100.0 : 0.0;
    }
}
