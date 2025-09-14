package org.seleznyov.iyu.kfin.ledger.application.service.arena;

import com.zaxxer.hikari.HikariConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

// Configuration
@Configuration
@EnableConfigurationProperties
public class LedgerConfiguration {

    @Bean
    public LedgerRingBuffer ledgerRingBuffer(NamedParameterJdbcTemplate jdbcTemplate) {
        return new LedgerRingBuffer(jdbcTemplate);
    }
    @Bean
    @Primary
    public Executor virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    // Hot accounts configuration - loaded from properties
    @Bean
    @ConfigurationProperties(prefix = "ledger.hot-accounts")
    public Set<UUID> hotAccountIds() {
        return new HashSet<>();
    }

    // Database configuration for optimal batch processing
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public HikariConfig hikariConfig() {
        HikariConfig config = new HikariConfig();
        config.setMaximumPoolSize(20); // Conservative for virtual threads
        config.setMinimumIdle(5);
        config.setConnectionTimeout(10000);
        config.setLeakDetectionThreshold(60000);
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "500");
        return config;
    }

    @Bean
    public NamedParameterJdbcTemplate namedParameterJdbcTemplate(DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }
}
