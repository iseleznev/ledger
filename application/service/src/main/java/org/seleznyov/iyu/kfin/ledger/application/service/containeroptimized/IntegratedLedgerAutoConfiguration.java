package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Auto-configuration for Integrated Container Optimized Ledger Service
 * Combines in-memory performance with database persistence
 */
@Configuration
@EnableConfigurationProperties(LedgerConfiguration.class)
@RequiredArgsConstructor
@Slf4j
public class IntegratedLedgerAutoConfiguration {

    private final LedgerConfiguration config;
    private final NamedParameterJdbcTemplate jdbcTemplate;

    @PostConstruct
    public void init() {
        log.info("Initializing Integrated Container Optimized Ledger Service...");
        config.validate();

        log.info("Ledger configuration: balance_shards={}, wal_shards={}, persistence_enabled=true",
            config.getOptimalShardCount(config.getBalanceStorage().getShardCount()),
            config.getOptimalShardCount(config.getWal().getShardCount()));
    }

    @Bean
    @Primary
    public Executor asyncExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    @Primary
    public ImmutableWalEntryRepository walEntryRepository(NamedParameterJdbcTemplate jdbcTemplate, Executor asyncExecutor) {
        return new ImmutableWalEntryRepository(jdbcTemplate, asyncExecutor);
    }

    @Bean
    @Primary
    public ImmutableEntryRecordRepository entryRecordRepository(NamedParameterJdbcTemplate jdbcTemplate, Executor asyncExecutor) {
        return new ImmutableEntryRecordRepository(jdbcTemplate, asyncExecutor);
    }

    @Bean
    @Primary
    public ImmutableEntrySnapshotRepository entrySnapshotRepository(NamedParameterJdbcTemplate jdbcTemplate, Executor asyncExecutor) {
        return new ImmutableEntrySnapshotRepository(jdbcTemplate, asyncExecutor);
    }

    @Bean
    @Primary
    public ImmutablePersistenceTransactionManager persistenceTransactionManager(
        ImmutableWalEntryRepository walEntryRepository,
        ImmutableEntryRecordRepository entryRecordRepository,
        ImmutableEntrySnapshotRepository entrySnapshotRepository) {
        return new ImmutablePersistenceTransactionManager(walEntryRepository, entryRecordRepository, entrySnapshotRepository);
    }

    @Bean
    @Primary
    public IntegratedContainerOptimizedWAL integratedWalStorage(ImmutableWalEntryRepository walEntryRepository) {
        return new IntegratedContainerOptimizedWAL(walEntryRepository);
    }

    @Bean
    @Primary
    public IntegratedContainerOptimizedLedgerService integratedLedgerService(
        ImmutablePersistenceTransactionManager persistenceManager,
        IntegratedContainerOptimizedWAL walStorage) {
        return new IntegratedContainerOptimizedLedgerService(persistenceManager, walStorage);
    }
}