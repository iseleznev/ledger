package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import org.springframework.stereotype.Component;

/**
 * Health indicator for WAL service monitoring
 */
@Component
public class LedgerWalHealthIndicator implements HealthIndicator {

    private final VirtualThreadLedgerWalService walService;
    private final ParameterMapPool parameterMapPool;

    public LedgerWalHealthIndicator(VirtualThreadLedgerWalService walService,
                                    ParameterMapPool parameterMapPool) {
        this.walService = walService;
        this.parameterMapPool = parameterMapPool;
    }

    @Override
    public Health health() {
        var healthBuilder = new Health.Builder();

        try {
            // Check ring buffer capacity
            double ringBufferUtilization = (double) (walService.getRingBufferSize() -
                walService.getRingBufferRemaining()) / walService.getRingBufferSize() * 100.0;

            // Check circuit breaker status
            boolean circuitBreakerOpen = walService.isCircuitBreakerOpen();
            long consecutiveFailures = walService.getConsecutiveFailures();

            // Check object pool efficiency
            double poolHitRate = parameterMapPool.getHitRate();

            healthBuilder
                .withDetail("ringBufferUtilization", String.format("%.1f%%", ringBufferUtilization))
                .withDetail("ringBufferRemaining", walService.getRingBufferRemaining())
                .withDetail("processedCount", walService.getProcessedCount())
                .withDetail("circuitBreakerOpen", circuitBreakerOpen)
                .withDetail("consecutiveFailures", consecutiveFailures)
                .withDetail("idempotencyCacheSize", walService.getIdempotencyCacheSize())
                .withDetail("objectPoolHitRate", String.format("%.1f%%", poolHitRate))
                .withDetail("objectPoolSize", parameterMapPool.getPoolSize());

            // Determine health status
            if (circuitBreakerOpen) {
                return healthBuilder.down()
                    .withDetail("reason", "Circuit breaker is open")
                    .build();
            }

            if (ringBufferUtilization > 95.0) {
                return healthBuilder.down()
                    .withDetail("reason", "Ring buffer nearly full")
                    .build();
            }

            if (consecutiveFailures > 3) {
                return healthBuilder.outOfService()
                    .withDetail("reason", "High failure rate detected")
                    .build();
            }

            return healthBuilder.up().build();

        } catch (Exception e) {
            return healthBuilder.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
