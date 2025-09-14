package org.seleznyov.iyu.kfin.ledger.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.model.ProcessingStrategy;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Main ledger service that coordinates between different processing strategies.
 * Acts as the primary entry point for all ledger operations and routes them
 * to appropriate strategy implementations based on account characteristics.
 * <p>
 * Strategy Selection Logic:
 * 1. Cold accounts (low volume) -> SimpleLedgerService with advisory locks
 * 2. Hot accounts (high volume) -> DisruptorLedgerService with LMAX Disruptor
 * <p>
 * The selection is made by AdaptiveStrategyService using ML-enhanced detection.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LedgerService {

    private final SimpleLedgerService simpleLedgerService;
    private final DisruptorLedgerService disruptorLedgerService;
    private final AdaptiveStrategyService adaptiveStrategyService;

    /**
     * Processes transfer between accounts using adaptive strategy selection.
     * The service automatically determines the optimal processing strategy
     * based on account activity patterns and system load.
     *
     * @param debitAccountId  Source account
     * @param creditAccountId Target account
     * @param amount          Transfer amount
     * @param currencyCode    Currency code
     * @param operationDate   Operation date
     * @param transactionId   Transaction identifier
     * @param idempotencyKey  Idempotency key
     */
    public void processTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        log.debug("Processing transfer through adaptive strategy: {} -> {}, amount: {}, tx: {}",
            debitAccountId, creditAccountId, amount, transactionId);

        // Determine processing strategy for debit account (primary decision factor)
        ProcessingStrategy strategy = adaptiveStrategyService.selectStrategy(
            debitAccountId, creditAccountId, amount
        );

        log.debug("Selected processing strategy: {} for accounts: {} -> {}",
            strategy, debitAccountId, creditAccountId);

        switch (strategy) {
            case SIMPLE -> {
                log.debug("Routing to SimpleLedgerService for cold account processing");
                simpleLedgerService.processTransfer(
                    debitAccountId, creditAccountId, amount, currencyCode,
                    operationDate, transactionId, idempotencyKey
                );
            }
            case DISRUPTOR -> {
                log.debug("Routing to DisruptorLedgerService for hot account processing");
                disruptorLedgerService.processTransfer(
                    debitAccountId, creditAccountId, amount, currencyCode,
                    operationDate, transactionId, idempotencyKey
                );
            }
            default -> throw new UnsupportedOperationException("Unknown processing strategy: " + strategy);
        }

        log.info("Transfer processed successfully using {} strategy: {} -> {}, amount: {}, tx: {}",
            strategy, debitAccountId, creditAccountId, amount, transactionId);
    }

    /**
     * Gets account balance using appropriate strategy.
     *
     * @param accountId     Account identifier
     * @param operationDate Date for balance calculation
     * @return Current balance
     */
    public long getBalance(UUID accountId, LocalDate operationDate) {
        log.debug("Getting balance through adaptive strategy for account: {}", accountId);

        ProcessingStrategy strategy = adaptiveStrategyService.selectStrategyForBalance(accountId);

        return switch (strategy) {
            case SIMPLE -> {
                log.debug("Using SimpleLedgerService for balance query");
                yield simpleLedgerService.getBalance(accountId, operationDate);
            }
            case DISRUPTOR -> {
                log.debug("Using DisruptorLedgerService for balance query");
                yield disruptorLedgerService.getBalance(accountId, operationDate);
            }
        };
    }

    /**
     * Gets comprehensive processing statistics from all strategies.
     *
     * @return Combined processing statistics
     */
    public CombinedProcessingStats getProcessingStats() {
        var simpleStats = simpleLedgerService.getProcessingStats();

        var disruptorStats = disruptorLedgerService.getProcessingStats();

        return new CombinedProcessingStats(
            simpleStats,
            disruptorStats,
            adaptiveStrategyService.getStrategyStats()
        );
    }

    /**
     * Forces strategy refresh for account (for testing/admin purposes).
     *
     * @param accountId Account to refresh strategy for
     */
    public void refreshAccountStrategy(UUID accountId) {
        log.info("Refreshing processing strategy for account: {}", accountId);
        adaptiveStrategyService.refreshAccountStrategy(accountId);
    }

    /**
     * Gets current strategy assignment for account (for monitoring).
     *
     * @param accountId Account to check
     * @return Currently assigned processing strategy
     */
    public ProcessingStrategy getCurrentStrategy(UUID accountId) {
        return adaptiveStrategyService.selectStrategyForBalance(accountId);
    }

    /**
     * Combined processing statistics from all strategies.
     */
    public record CombinedProcessingStats(
        SimpleLedgerService.ProcessingStats simpleStats,
        DisruptorLedgerService.ProcessingStats disruptorStats,
        AdaptiveStrategyService.StrategyStats strategyStats
    ) {

    }
}