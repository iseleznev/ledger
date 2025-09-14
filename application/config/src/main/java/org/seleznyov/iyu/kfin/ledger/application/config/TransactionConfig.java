package org.seleznyov.iyu.kfin.ledger.application.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import lombok.extern.slf4j.Slf4j;

/**
 * Transaction configuration for ledger system.
 * Provides proper ACID transaction management using DataSourceTransactionManager.
 *
 * CRITICAL: This replaces the dangerous NoOpTransactionManager that completely
 * disabled transaction management. Real database transactions are essential for
 * ledger operations to maintain data consistency and prevent race conditions.
 *
 * Features:
 * - DataSourceTransactionManager for proper JDBC transaction handling
 * - TransactionTemplate for programmatic transaction management
 * - Conditional beans to avoid conflicts in multi-module setup
 * - Proper error handling and logging
 */
@Configuration
@EnableTransactionManagement(proxyTargetClass = true)
@Slf4j
public class TransactionConfig {

    /**
     * Primary transaction manager for JDBC operations.
     * Uses DataSourceTransactionManager for proper ACID transaction handling.
     *
     * @param dataSource The DataSource to manage transactions for
     * @return Configured DataSourceTransactionManager
     */
    @Bean(name = "transactionManager")
    @ConditionalOnMissingBean(PlatformTransactionManager.class)
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        log.info("Configuring DataSourceTransactionManager for JDBC operations");

        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);

        // Configure transaction manager properties
        transactionManager.setDefaultTimeout(30); // 30 seconds default timeout
        transactionManager.setRollbackOnCommitFailure(true);
        transactionManager.setEnforceReadOnly(true); // Enforce read-only for @Transactional(readOnly=true)
        transactionManager.setValidateExistingTransaction(true); // Validate nested transactions

        // Enable global rollback on any exception (not just RuntimeException)
        transactionManager.setGlobalRollbackOnParticipationFailure(true);

        log.info("DataSourceTransactionManager configured successfully with 30s timeout");
        return transactionManager;
    }

    /**
     * TransactionTemplate for programmatic transaction management.
     * Useful for complex transaction scenarios that require programmatic control.
     *
     * @param transactionManager The transaction manager to use
     * @return Configured TransactionTemplate
     */
    @Bean
    @ConditionalOnMissingBean(TransactionTemplate.class)
    public TransactionTemplate transactionTemplate(PlatformTransactionManager transactionManager) {
        log.debug("Configuring TransactionTemplate for programmatic transaction management");

        TransactionTemplate template = new TransactionTemplate(transactionManager);

        // Configure default properties
        template.setTimeout(30); // Match transaction manager timeout
        template.setReadOnly(false); // Default to read-write
        template.setIsolationLevel(TransactionTemplate.ISOLATION_READ_COMMITTED); // Safe default
        template.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRED); // Standard behavior

        return template;
    }

    /**
     * TransactionTemplate for read-only operations.
     * Optimized for balance queries and reporting operations.
     *
     * @param transactionManager The transaction manager to use
     * @return Read-only TransactionTemplate
     */
    @Bean(name = "readOnlyTransactionTemplate")
    public TransactionTemplate readOnlyTransactionTemplate(PlatformTransactionManager transactionManager) {
        log.debug("Configuring read-only TransactionTemplate for balance queries");

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setReadOnly(true);
        template.setTimeout(10); // Shorter timeout for read operations
        template.setIsolationLevel(TransactionTemplate.ISOLATION_READ_COMMITTED);
        template.setPropagationBehavior(TransactionTemplate.PROPAGATION_SUPPORTS);

        return template;
    }

    /**
     * TransactionTemplate for high-priority operations with extended timeout.
     * Used for snapshot creation and other long-running operations.
     *
     * @param transactionManager The transaction manager to use
     * @return Long-running TransactionTemplate
     */
    @Bean(name = "longRunningTransactionTemplate")
    public TransactionTemplate longRunningTransactionTemplate(PlatformTransactionManager transactionManager) {
        log.debug("Configuring long-running TransactionTemplate for snapshots");

        TransactionTemplate template = new TransactionTemplate(transactionManager);
        template.setTimeout(120); // 2 minutes for complex operations
        template.setReadOnly(false);
        template.setIsolationLevel(TransactionTemplate.ISOLATION_READ_COMMITTED);
        template.setPropagationBehavior(TransactionTemplate.PROPAGATION_REQUIRED);

        return template;
    }
}