package org.seleznyov.iyu.kfin.ledger.application.usecase;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.service.ValidationService;
import org.seleznyov.iyu.kfin.ledger.application.service.LedgerService;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Use case for processing money transfers between accounts.
 * Coordinates validation and delegates to appropriate ledger service strategy.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ProcessTransferUseCase {

    private final ValidationService validationService;
    private final LedgerService ledgerService;

    /**
     * Processes transfer from debit account to credit account.
     *
     * @param request Transfer request parameters
     * @return Transfer result with success status and metadata
     */
    @Transactional
    public TransferResult processTransfer(TransferRequest request) {
        log.debug("Processing transfer: {} -> {}, amount: {}, transaction: {}",
            request.debitAccountId(), request.creditAccountId(),
            request.amount(), request.transactionId());

        try {
            // 1. Validate request parameters and business rules
            validationService.validateTransfer(
                request.debitAccountId(),
                request.creditAccountId(),
                request.amount(),
                request.currencyCode(),
                request.operationDate(),
                request.transactionId(),
                request.idempotencyKey()
            );

            // 2. Delegate to ledger service (will choose appropriate strategy)
            ledgerService.processTransfer(
                request.debitAccountId(),
                request.creditAccountId(),
                request.amount(),
                request.currencyCode(),
                request.operationDate(),
                request.transactionId(),
                request.idempotencyKey()
            );

            log.info("Transfer processed successfully: transaction {}", request.transactionId());

            return TransferResult.success(
                request.transactionId(),
                request.idempotencyKey(),
                "Transfer processed successfully"
            );

        } catch (ValidationService.InsufficientFundsException e) {
            log.warn("Transfer failed - insufficient funds: {}", e.getMessage());
            return TransferResult.failure(
                request.transactionId(),
                request.idempotencyKey(),
                TransferResult.FailureReason.INSUFFICIENT_FUNDS,
                e.getMessage()
            );

        } catch (ValidationService.ValidationException e) {
            log.warn("Transfer failed - validation error: {}", e.getMessage());
            return TransferResult.failure(
                request.transactionId(),
                request.idempotencyKey(),
                TransferResult.FailureReason.VALIDATION_ERROR,
                e.getMessage()
            );

        } catch (Exception e) {
            log.error("Transfer failed - system error: {}", e.getMessage(), e);
            return TransferResult.failure(
                request.transactionId(),
                request.idempotencyKey(),
                TransferResult.FailureReason.SYSTEM_ERROR,
                "Internal system error occurred"
            );
        }
    }

    /**
     * Transfer request parameters.
     */
    public record TransferRequest(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        /**
         * Creates transfer request with current date.
         */
        public static TransferRequest of(
            UUID debitAccountId,
            UUID creditAccountId,
            long amount,
            String currencyCode,
            UUID transactionId,
            UUID idempotencyKey
        ) {
            return new TransferRequest(
                debitAccountId,
                creditAccountId,
                amount,
                currencyCode,
                LocalDate.now(),
                transactionId,
                idempotencyKey
            );
        }
    }

    /**
     * Transfer processing result.
     */
    public record TransferResult(
        UUID transactionId,
        UUID idempotencyKey,
        boolean success,
        FailureReason failureReason,
        String message,
        long processingTimeMs
    ) {

        public static TransferResult success(
            UUID transactionId,
            UUID idempotencyKey,
            String message
        ) {
            return new TransferResult(
                transactionId,
                idempotencyKey,
                true,
                null,
                message,
                System.currentTimeMillis()
            );
        }

        public static TransferResult failure(
            UUID transactionId,
            UUID idempotencyKey,
            FailureReason reason,
            String message
        ) {
            return new TransferResult(
                transactionId,
                idempotencyKey,
                false,
                reason,
                message,
                System.currentTimeMillis()
            );
        }

        public enum FailureReason {
            INSUFFICIENT_FUNDS,
            VALIDATION_ERROR,
            IDEMPOTENCY_VIOLATION,
            ACCOUNT_LOCKED,
            SYSTEM_ERROR
        }
    }
}