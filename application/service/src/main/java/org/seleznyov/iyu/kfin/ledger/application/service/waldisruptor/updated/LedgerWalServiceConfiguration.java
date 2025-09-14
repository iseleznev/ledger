package org.seleznyov.iyu.kfin.ledger.application.service.waldisruptor.updated;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Production-ready configuration with application.yml integration
 */
@Service
@Slf4j
public class LedgerWalServiceConfiguration {

    /**
     * Initialize WAL service with proper configuration
     */
    public void initializeWalServices(VirtualThreadLedgerWalService walService,
                                      VirtualThreadWalProcessorService walProcessor) {

        // Start WAL processor with monitoring
        walProcessor.startScheduledProcessing();

        // Log configuration
        log.info("=== Ledger WAL Service Production Configuration ===");
        log.info("All services initialized and ready for production traffic");
        log.info("Health checks available at /actuator/health/ledgerWal");
        log.info("Metrics available at /actuator/metrics/ledger.*");

        // Register shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Graceful shutdown initiated...");
            try {
                walProcessor.close();
                walService.close();
                log.info("Graceful shutdown completed");
            } catch (Exception e) {
                log.error("Error during graceful shutdown", e);
            }
        }));
    }
}
