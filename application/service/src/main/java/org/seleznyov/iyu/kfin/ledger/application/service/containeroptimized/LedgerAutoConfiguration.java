package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Auto-configuration for Container Optimized Ledger Service
 */
@Configuration
@EnableConfigurationProperties(LedgerConfiguration.class)
@RequiredArgsConstructor
@Slf4j
public class LedgerAutoConfiguration {

    private final LedgerConfiguration config;

    @PostConstruct
    public void init() {
        log.info("Initializing Container Optimized Ledger Service...");
        config.validate();

        log.info("Ledger configuration: balance_shards={}, wal_shards={}, snapshot_shards={}",
            config.getOptimalShardCount(config.getBalanceStorage().getShardCount()),
            config.getOptimalShardCount(config.getWal().getShardCount()),
            config.getOptimalShardCount(config.getSnapshot().getShardCount()));
    }

    @Bean
    @Primary
    public ContainerOptimizedBalanceStorage balanceStorage() {
        return new ContainerOptimizedBalanceStorage();
    }

    @Bean
    @Primary
    public ContainerOptimizedWAL walStorage() {
        return new ContainerOptimizedWAL();
    }

    @Bean
    @Primary
    public ContainerOptimizedSnapshotService snapshotService() {
        return new ContainerOptimizedSnapshotService();
    }

    @Bean
    @Primary
    public ContainerOptimizedLedgerService ledgerService(
        ContainerOptimizedBalanceStorage balanceStorage,
        ContainerOptimizedWAL walStorage,
        ContainerOptimizedSnapshotService snapshotService) {
        return new ContainerOptimizedLedgerService();
    }

    @Bean
    public LedgerMetrics ledgerMetrics() {
        return new LedgerMetrics();
    }

    @Bean
    public LedgerRecoveryService recoveryService(
        ContainerOptimizedLedgerService ledgerService,
        ContainerOptimizedBalanceStorage balanceStorage,
        ContainerOptimizedWAL walStorage,
        ContainerOptimizedSnapshotService snapshotService) {
        return new LedgerRecoveryService(ledgerService, balanceStorage, walStorage, snapshotService);
    }

//    @Bean
//    @ConditionalOnProperty(name = "ledger.container-optimized.performance.enable-performance-testing", havingValue = "true")
//    public LedgerPerformanceTest performanceTest(ContainerOptimizedLedgerService ledgerService) {
//        return new LedgerPerformanceTest(ledgerService);
//    }
//
    @Bean
    public LedgerController ledgerController(ContainerOptimizedLedgerService ledgerService) {
        return new LedgerController(ledgerService);
    }

    @PreDestroy
    public void cleanup() {
        log.info("Shutting down Container Optimized Ledger Service...");
    }
}