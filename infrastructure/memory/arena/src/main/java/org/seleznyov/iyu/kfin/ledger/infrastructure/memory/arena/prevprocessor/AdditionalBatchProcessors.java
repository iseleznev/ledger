package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.batchprocessor;

import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.DirectPostgresBatchSender;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.handler.PostgresBinaryBatchLayout;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.LoggingRingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.MetricsRingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.RetryRingBufferProcessor;
import org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.prevprocessor.ValidationRingBufferProcessor;

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
            new ValidationRingBufferProcessor(baseProcessor);

        // Добавляем retry
        PostgresBinaryBatchLayout.BatchProcessor retryingProcessor =
            new RetryRingBufferProcessor(validatingProcessor, 3, 100);

        // Добавляем metrics
        PostgresBinaryBatchLayout.BatchProcessor metricsProcessor =
            new MetricsRingBufferProcessor(retryingProcessor);

        // Добавляем logging для важных операций
        return new LoggingRingBufferProcessor(metricsProcessor);
    }
