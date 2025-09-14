package org.seleznyov.iyu.kfin.ledger.application.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.service.BalanceCalculationService;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.AdvisoryLockRepository;
import org.seleznyov.iyu.kfin.ledger.infrastructure.persistence.api.repository.EntryRecordRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Simple ledger service implementation for cold accounts using PostgreSQL advisory locks.
 * This service handles accounts with low transaction volume using traditional database locks.
 *
 * Key characteristics:
 * - Uses advisory locks only on debit account (to prevent deadlocks)
 * - Synchronous processing with immediate persistence
 * - Balance validation before transfer execution
 * - Optimized for correctness over throughput (~1000 ops/sec)
 * - Integrates with ML detector for strategy learning
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimpleLedgerService {

    private final AdvisoryLockRepository advisoryLockRepository;
    private final EntryRecordRepository entryRecordRepository;
    private final BalanceCalculationService balanceCalculationService;
    private final AdaptiveStrategyService adaptiveStrategyService;

    /**
     * Processes transfer using simple advisory lock strategy.
     *
     * Lock strategy:
     * - Acquire advisory lock ONLY on debit account (source of funds)
     * - No locks on credit account (prevents circular deadlocks)
     * - Lock is automatically released at transaction end
     *
     * @param debitAccountId Source account
     * @param creditAccountId Target account
     * @param amount Transfer amount (in cents/smallest currency unit)
     * @param currencyCode Currency code (USD, EUR, etc.)
     * @param operationDate Operation date
     * @param transactionId Unique transaction identifier
     * @param idempotencyKey Idempotency key for duplicate prevention
     */
    @Transactional
    public void processTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        log.debug("Processing simple transfer: {} -> {}, amount: {}, tx: {}",
            debitAccountId, creditAccountId, amount, transactionId);

        // Record transactions for ML learning before processing
        adaptiveStrategyService.recordTransaction(debitAccountId, amount, true);  // debit
        adaptiveStrategyService.recordTransaction(creditAccountId, amount, false); // credit

        // 1. Acquire advisory lock on debit account only
        boolean lockAcquired = advisoryLockRepository.tryTransactionalLock(debitAccountId);
        if (!lockAcquired) {
            log.warn("Failed to acquire lock for debit account: {}, tx: {}",
                debitAccountId, transactionId);
            throw new ConcurrentOperationException(
                "Account " + debitAccountId + " is currently locked by another operation"
            );
        }

        log.debug("Acquired advisory lock for debit account: {}", debitAccountId);

        try {
            // 2. Check sufficient balance (within lock protection)
            long currentBalance = balanceCalculationService.calculateBalance(
                debitAccountId, operationDate
            );

            if (currentBalance < amount) {
                log.warn("Insufficient balance for transfer: account={}, required={}, available={}, tx={}",
                    debitAccountId, amount, currentBalance, transactionId);
                throw new InsufficientFundsException(
                    String.format("Insufficient balance: required=%d, available=%d", amount, currentBalance)
                );
            }

            // 3. Execute double-entry insert
            entryRecordRepository.insert(
                debitAccountId,
                creditAccountId,
                operationDate,
                transactionId,
                amount,
                currencyCode,
                idempotencyKey
            );

            log.info("Simple transfer completed successfully: {} -> {}, amount: {}, tx: {}",
                debitAccountId, creditAccountId, amount, transactionId);

        } catch (Exception e) {
            log.error("Simple transfer failed: {} -> {}, amount: {}, tx: {}, error: {}",
                debitAccountId, creditAccountId, amount, transactionId, e.getMessage());
            throw e; // Re-throw to trigger transaction rollback
        }
        // Advisory lock is automatically released when transaction commits/rolls back
    }

    /**
     * Gets current balance for account (used by cold account strategy).
     *
     * @param accountId Account identifier
     * @param operationDate Date for balance calculation
     * @return Current balance
     */
    @Transactional(readOnly = true)
    public long getBalance(UUID accountId, LocalDate operationDate) {
        log.debug("Getting balance for cold account: {}, date: {}", accountId, operationDate);

        long balance = balanceCalculationService.calculateBalance(accountId, operationDate);

        log.debug("Balance retrieved for cold account {}: {}", accountId, balance);
        return balance;
    }

    /**
     * Checks if account can be processed by simple ledger service.
     * This is a fallback service that can handle any account.
     *
     * @param accountId Account to check
     * @return true (simple service can process any account)
     */
    public boolean canProcessAccount(UUID accountId) {
        return true; // Simple service is the fallback for all accounts
    }

    /**
     * Gets processing statistics for monitoring.
     *
     * @return Processing statistics
     */
    public ProcessingStats getProcessingStats() {
        // Placeholder for metrics collection
        return new ProcessingStats(
            "SIMPLE",
            0, // processed count - would be collected from metrics
            0, // failed count
            0.0 // average processing time
        );
    }

    /**
     * Exception thrown when account is locked by another operation.
     */
    public static class ConcurrentOperationException extends RuntimeException {
        public ConcurrentOperationException(String message) {
            super(message);
        }
    }

    /**
     * Exception thrown when account has insufficient balance.
     */
    public static class InsufficientFundsException extends RuntimeException {
        public InsufficientFundsException(String message) {
            super(message);
        }
    }

    /**
     * Processing statistics for monitoring.
     */
    public record ProcessingStats(
        String strategy,
        long processedCount,
        long failedCount,
        double averageProcessingTimeMs
    ) {}
}