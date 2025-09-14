package org.seleznyov.iyu.kfin.ledger.application.usecase;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.domain.service.BalanceCalculationService;
import org.seleznyov.iyu.kfin.ledger.domain.service.ValidationService;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Use case for retrieving account balance information.
 * Provides both simple balance queries and detailed breakdowns for monitoring.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class GetBalanceUseCase {

    private final BalanceCalculationService balanceCalculationService;
    private final ValidationService validationService;

    /**
     * Gets current balance for account as of specified date.
     *
     * @param request Balance inquiry request
     * @return Balance inquiry result
     */
    @Transactional(readOnly = true)
    public BalanceResult getBalance(BalanceRequest request) {
        log.debug("Getting balance for account: {}, date: {}",
            request.accountId(), request.operationDate());

        try {
            // Validate request parameters
            validationService.validateBalanceInquiry(
                request.accountId(),
                request.operationDate()
            );

            // Calculate balance
            long balance = balanceCalculationService.calculateBalance(
                request.accountId(),
                request.operationDate()
            );

            log.debug("Balance retrieved for account {}: {}", request.accountId(), balance);

            return BalanceResult.success(
                request.accountId(),
                balance,
                request.operationDate(),
                "Balance retrieved successfully"
            );

        } catch (ValidationService.ValidationException e) {
            log.warn("Balance inquiry failed - validation error: {}", e.getMessage());
            return BalanceResult.failure(
                request.accountId(),
                request.operationDate(),
                BalanceResult.FailureReason.VALIDATION_ERROR,
                e.getMessage()
            );

        } catch (Exception e) {
            log.error("Balance inquiry failed - system error: {}", e.getMessage(), e);
            return BalanceResult.failure(
                request.accountId(),
                request.operationDate(),
                BalanceResult.FailureReason.SYSTEM_ERROR,
                "Internal system error occurred"
            );
        }
    }

    /**
     * Gets detailed balance breakdown for monitoring and debugging.
     *
     * @param request Balance inquiry request
     * @return Detailed balance breakdown result
     */
    @Transactional(readOnly = true)
    public DetailedBalanceResult getDetailedBalance(BalanceRequest request) {
        log.debug("Getting detailed balance for account: {}, date: {}",
            request.accountId(), request.operationDate());

        try {
            // Validate request parameters
            validationService.validateBalanceInquiry(
                request.accountId(),
                request.operationDate()
            );

            // Get detailed breakdown
            var breakdown = balanceCalculationService.calculateBalanceWithBreakdown(
                request.accountId(),
                request.operationDate()
            );

            log.debug("Detailed balance retrieved for account {}: current={}, snapshot={}, delta={}",
                request.accountId(), breakdown.currentBalance(),
                breakdown.snapshotBalance(), breakdown.deltaFromSnapshot());

            return DetailedBalanceResult.success(
                request.accountId(),
                request.operationDate(),
                breakdown,
                "Detailed balance retrieved successfully"
            );

        } catch (ValidationService.ValidationException e) {
            log.warn("Detailed balance inquiry failed - validation error: {}", e.getMessage());
            return DetailedBalanceResult.failure(
                request.accountId(),
                request.operationDate(),
                DetailedBalanceResult.FailureReason.VALIDATION_ERROR,
                e.getMessage()
            );

        } catch (Exception e) {
            log.error("Detailed balance inquiry failed - system error: {}", e.getMessage(), e);
            return DetailedBalanceResult.failure(
                request.accountId(),
                request.operationDate(),
                DetailedBalanceResult.FailureReason.SYSTEM_ERROR,
                "Internal system error occurred"
            );
        }
    }

    /**
     * Balance inquiry request parameters.
     */
    public record BalanceRequest(
        UUID accountId,
        LocalDate operationDate
    ) {
        /**
         * Creates balance request for current date.
         */
        public static BalanceRequest of(UUID accountId) {
            return new BalanceRequest(accountId, LocalDate.now());
        }
    }

    /**
     * Simple balance inquiry result.
     */
    public record BalanceResult(
        UUID accountId,
        LocalDate operationDate,
        Long balance,
        boolean success,
        FailureReason failureReason,
        String message,
        long queryTimeMs
    ) {

        public static BalanceResult success(
            UUID accountId,
            long balance,
            LocalDate operationDate,
            String message
        ) {
            return new BalanceResult(
                accountId,
                operationDate,
                balance,
                true,
                null,
                message,
                System.currentTimeMillis()
            );
        }

        public static BalanceResult failure(
            UUID accountId,
            LocalDate operationDate,
            FailureReason reason,
            String message
        ) {
            return new BalanceResult(
                accountId,
                operationDate,
                null,
                false,
                reason,
                message,
                System.currentTimeMillis()
            );
        }

        public enum FailureReason {
            VALIDATION_ERROR,
            ACCOUNT_NOT_FOUND,
            SYSTEM_ERROR
        }
    }

    /**
     * Detailed balance inquiry result with breakdown.
     */
    public record DetailedBalanceResult(
        UUID accountId,
        LocalDate operationDate,
        BalanceCalculationService.BalanceBreakdown breakdown,
        boolean success,
        FailureReason failureReason,
        String message,
        long queryTimeMs
    ) {

        public static DetailedBalanceResult success(
            UUID accountId,
            LocalDate operationDate,
            BalanceCalculationService.BalanceBreakdown breakdown,
            String message
        ) {
            return new DetailedBalanceResult(
                accountId,
                operationDate,
                breakdown,
                true,
                null,
                message,
                System.currentTimeMillis()
            );
        }

        public static DetailedBalanceResult failure(
            UUID accountId,
            LocalDate operationDate,
            FailureReason reason,
            String message
        ) {
            return new DetailedBalanceResult(
                accountId,
                operationDate,
                null,
                false,
                reason,
                message,
                System.currentTimeMillis()
            );
        }

        public enum FailureReason {
            VALIDATION_ERROR,
            ACCOUNT_NOT_FOUND,
            SYSTEM_ERROR
        }
    }
}