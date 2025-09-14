package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.processor.batchprocessor;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.DirectPostgresBatchSender;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;

/**
 * ✅ Дополнительные реализации BatchProcessor для различных сценариев
 */

/**
     * ✅ Composite BatchProcessor - объединяет несколько processor'ов
     */
    public static PostgresBinaryBatchLayout.BatchProcessor createCompositeProcessor(
        DirectPostgresBatchSender directSender, int workerId) {

        // Базовый processor
        PostgresBinaryBatchLayout.BatchProcessor baseProcessor =
            PostgresBinaryBatchLayout.createPostgresProcessor(directSender, workerId);

        // Добавляем validation
        PostgresBinaryBatchLayout.BatchProcessor validatingProcessor =
            new ValidationBatchProcessor(baseProcessor);

        // Добавляем retry
        PostgresBinaryBatchLayout.BatchProcessor retryingProcessor =
            new RetryBatchProcessor(validatingProcessor, 3, 100);

        // Добавляем metrics
        PostgresBinaryBatchLayout.BatchProcessor metricsProcessor =
            new MetricsBatchProcessor(retryingProcessor);

        // Добавляем logging для важных операций
        return new LoggingBatchProcessor(metricsProcessor);
    }
