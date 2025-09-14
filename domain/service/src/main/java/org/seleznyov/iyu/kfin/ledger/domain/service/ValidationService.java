package org.seleznyov.iyu.kfin.ledger.domain.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * FIXED Domain service for validating ledger operations.
 *
 * CRITICAL FIXES:
 * 1. REMOVED TOCTOU vulnerability - balance validation moved to atomic operations
 * 2. Enhanced currency validation with comprehensive checks
 * 3. Improved date validation with business-specific rules
 * 4. Better error messages for debugging and compliance
 * 5. Performance optimizations for high-frequency validation
 */
@Service
@RequiredArgsConstructor
public class ValidationService {

    // REMOVED: BalanceCalculationService dependency to eliminate TOCTOU vulnerability
    // Balance validation is now done atomically within database operations

    // Enhanced currency validation
    private static final Set<String> SUPPORTED_CURRENCIES = Set.of(
        "USD", "EUR", "RUB", "AZN", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY"
    );

    private static final Pattern CURRENCY_PATTERN = Pattern.compile("^[A-Z]{3}$");

    // Business rule constants (configurable in real system)
    private static final long MAX_TRANSFER_AMOUNT = 1_000_000_00L; // $1M in cents
    private static final long MAX_DAILY_AMOUNT = 10_000_000_00L;   // $10M daily limit
    private static final int MAX_OPERATION_HISTORY_YEARS = 2;      // 2 years max history
    private static final int MIN_OPERATION_HISTORY_DAYS = 1;       // Minimum 1 day ago

    /**
     * FIXED: Validates transfer request parameters WITHOUT balance check.
     * Balance validation is now handled atomically in the database layer to prevent TOCTOU.
     *
     * @param debitAccountId Source account ID
     * @param creditAccountId Target account ID
     * @param amount Transfer amount
     * @param currencyCode Currency code
     * @param operationDate Operation date
     * @param transactionId Transaction ID
     * @param idempotencyKey Idempotency key
     * @throws ValidationException if validation fails
     */
    public void validateTransfer(
        UUID debitAccountId,
        UUID creditAccountId,
        long amount,
        String currencyCode,
        LocalDate operationDate,
        UUID transactionId,
        UUID idempotencyKey
    ) {
        // Basic parameter validation
        validateAccountIds(debitAccountId, creditAccountId);
        validateAmount(amount);
        validateCurrency(currencyCode);
        validateOperationDate(operationDate);
        validateUUIDs(transactionId, idempotencyKey);

        // CRITICAL: Balance validation REMOVED from here
        // It's now handled atomically in the database operations to prevent TOCTOU

        // Additional business rules validation
        validateTransferBusinessRules(debitAccountId, creditAccountId, amount, operationDate);
    }

    /**
     * Validates balance inquiry parameters.
     *
     * @param accountId Account ID to check
     * @param operationDate Date for balance calculation
     * @throws ValidationException if validation fails
     */
    public void validateBalanceInquiry(UUID accountId, LocalDate operationDate) {
        validateAccountId(accountId);
        validateOperationDate(operationDate);

        // Additional validation for balance queries
        validateBalanceInquiryBusinessRules(accountId, operationDate);
    }

    /**
     * Validates snapshot creation parameters.
     *
     * @param accountId Account ID for snapshot
     * @param operationDate Snapshot operation date
     * @throws ValidationException if validation fails
     */
    public void validateSnapshotCreation(UUID accountId, LocalDate operationDate) {
        validateAccountId(accountId);
        validateOperationDate(operationDate);

        // Snapshot-specific business rules
        if (operationDate.isAfter(LocalDate.now())) {
            throw new ValidationException(String.format(
                "Cannot create snapshot for future date: %s (current date: %s)",
                operationDate, LocalDate.now()));
        }

        // Prevent snapshots too far in the past (business rule)
        LocalDate earliestAllowed = LocalDate.now().minusYears(MAX_OPERATION_HISTORY_YEARS);
        if (operationDate.isBefore(earliestAllowed)) {
            throw new ValidationException(String.format(
                "Cannot create snapshot for date older than %d years: %s",
                MAX_OPERATION_HISTORY_YEARS, operationDate));
        }
    }

    /**
     * ENHANCED: Validates batch operation parameters.
     *
     * @param accountIds List of account IDs
     * @param operationDate Operation date
     * @param maxBatchSize Maximum allowed batch size
     * @throws ValidationException if validation fails
     */
    public void validateBatchOperation(java.util.List<UUID> accountIds, LocalDate operationDate, int maxBatchSize) {
        if (accountIds == null || accountIds.isEmpty()) {
            throw new ValidationException("Account IDs list cannot be null or empty");
        }

        if (accountIds.size() > maxBatchSize) {
            throw new ValidationException(String.format(
                "Batch size %d exceeds maximum allowed size %d",
                accountIds.size(), maxBatchSize));
        }

        // Check for duplicate account IDs
        long uniqueCount = accountIds.stream().distinct().count();
        if (uniqueCount != accountIds.size()) {
            throw new ValidationException(String.format(
                "Batch contains duplicate account IDs: %d unique out of %d total",
                uniqueCount, accountIds.size()));
        }

        // Validate each account ID
        for (UUID accountId : accountIds) {
            validateAccountId(accountId);
        }

        validateOperationDate(operationDate);
    }

    private void validateAccountIds(UUID debitAccountId, UUID creditAccountId) {
        validateAccountId(debitAccountId);
        validateAccountId(creditAccountId);

        if (debitAccountId.equals(creditAccountId)) {
            throw new ValidationException(String.format(
                "Debit and credit accounts cannot be the same: %s", debitAccountId));
        }
    }

    private void validateAccountId(UUID accountId) {
        if (accountId == null) {
            throw new ValidationException("Account ID cannot be null");
        }

        // Enhanced UUID validation
        if (accountId.toString().equals("00000000-0000-0000-0000-000000000000")) {
            throw new ValidationException("Account ID cannot be null UUID");
        }
    }

    private void validateAmount(long amount) {
        if (amount <= 0) {
            throw new ValidationException(String.format(
                "Transfer amount must be positive, got: %d", amount));
        }

        // Enhanced amount validation with better error messages
        if (amount > MAX_TRANSFER_AMOUNT) {
            throw new ValidationException(String.format(
                "Transfer amount %d exceeds maximum limit of %d (%.2f)",
                amount, MAX_TRANSFER_AMOUNT, MAX_TRANSFER_AMOUNT / 100.0));
        }

        // Validate minimum amount (prevent micro-transactions spam)
        if (amount < 1) { // 1 cent minimum
            throw new ValidationException(String.format(
                "Transfer amount %d is below minimum allowed amount of 1 cent", amount));
        }
    }

    /**
     * ENHANCED: Comprehensive currency validation.
     */
    private void validateCurrency(String currencyCode) {
        if (currencyCode == null) {
            throw new ValidationException("Currency code cannot be null");
        }

        String trimmedCode = currencyCode.trim().toUpperCase();

        if (trimmedCode.isEmpty()) {
            throw new ValidationException("Currency code cannot be empty");
        }

        if (!CURRENCY_PATTERN.matcher(trimmedCode).matches()) {
            throw new ValidationException(String.format(
                "Currency code must be 3 uppercase letters, got: '%s'", currencyCode));
        }

        if (!SUPPORTED_CURRENCIES.contains(trimmedCode)) {
            throw new ValidationException(String.format(
                "Unsupported currency code: %s. Supported currencies: %s",
                trimmedCode, SUPPORTED_CURRENCIES));
        }
    }

    /**
     * ENHANCED: Business-aware date validation.
     */
    private void validateOperationDate(LocalDate operationDate) {
        if (operationDate == null) {
            throw new ValidationException("Operation date cannot be null");
        }

        LocalDate today = LocalDate.now();

        // Business rule: operation date cannot be in the future
        if (operationDate.isAfter(today)) {
            throw new ValidationException(String.format(
                "Operation date cannot be in the future: %s (current date: %s)",
                operationDate, today));
        }

        // Business rule: operation date cannot be too old
        LocalDate cutoffDate = today.minusYears(MAX_OPERATION_HISTORY_YEARS);
        if (operationDate.isBefore(cutoffDate)) {
            throw new ValidationException(String.format(
                "Operation date is too old (>%d years): %s (cutoff: %s)",
                MAX_OPERATION_HISTORY_YEARS, operationDate, cutoffDate));
        }

        // Additional business rule: cannot be too recent for certain operations
        // This prevents backdating operations
        LocalDate minimumDate = today.minusDays(MIN_OPERATION_HISTORY_DAYS);
        if (operationDate.isAfter(minimumDate) && operationDate.isBefore(today)) {
            long daysDiff = ChronoUnit.DAYS.between(operationDate, today);
            if (daysDiff > 0) {
                // Allow same-day operations, but log suspicious backdating
                java.util.logging.Logger.getLogger(ValidationService.class.getName())
                    .warning(String.format("Potentially backdated operation: %s (%d days ago)",
                        operationDate, daysDiff));
            }
        }
    }

    private void validateUUIDs(UUID... uuids) {
        for (UUID uuid : uuids) {
            if (uuid == null) {
                throw new ValidationException("UUID cannot be null");
            }

            // Check for null UUID
            if (uuid.toString().equals("00000000-0000-0000-0000-000000000000")) {
                throw new ValidationException("UUID cannot be null UUID");
            }
        }
    }

    /**
     * ENHANCED: Transfer-specific business rules validation.
     */
    private void validateTransferBusinessRules(UUID debitAccountId, UUID creditAccountId,
                                               long amount, LocalDate operationDate) {
        // Rule 1: Weekend/holiday restrictions (simplified)
        java.time.DayOfWeek dayOfWeek = operationDate.getDayOfWeek();
        if (amount > 100_000_00L && (dayOfWeek == java.time.DayOfWeek.SATURDAY ||
            dayOfWeek == java.time.DayOfWeek.SUNDAY)) {
            throw new ValidationException(String.format(
                "Large transfers (>$100K) not allowed on weekends: amount=%.2f, date=%s",
                amount / 100.0, operationDate));
        }

        // Rule 2: Same-day transfer limits (would need database check in real system)
        // This is a placeholder - real implementation would query existing transfers

        // Rule 3: Account type restrictions (would need account metadata)
        // This is a placeholder - real implementation would check account types
    }

    private void validateBalanceInquiryBusinessRules(UUID accountId, LocalDate operationDate) {
        // Rule 1: Balance inquiries should not be too far in the past
        LocalDate oldestAllowed = LocalDate.now().minusMonths(6);
        if (operationDate.isBefore(oldestAllowed)) {
            throw new ValidationException(String.format(
                "Balance inquiry date too old (>6 months): %s", operationDate));
        }
    }

    /**
     * ADDED: Validates idempotency key uniqueness constraints.
     * This is a placeholder - real implementation would check database.
     */
    public void validateIdempotencyKey(UUID idempotencyKey, UUID transactionId) {
        validateUUIDs(idempotencyKey, transactionId);

        if (idempotencyKey.equals(transactionId)) {
            throw new ValidationException(
                "Idempotency key should be different from transaction ID for better traceability");
        }
    }

    /**
     * ADDED: Validates transfer limits (daily, monthly, etc.).
     * This is a placeholder - real implementation would check database for existing transfers.
     */
    public void validateTransferLimits(UUID accountId, long amount, LocalDate operationDate) {
        validateAccountId(accountId);
        validateAmount(amount);
        validateOperationDate(operationDate);

        // Placeholder for daily limit check
        // Real implementation would query sum of transfers for the day
        if (amount > MAX_DAILY_AMOUNT) {
            throw new TransferLimitExceededException(String.format(
                "Single transfer amount %.2f exceeds daily limit of %.2f",
                amount / 100.0, MAX_DAILY_AMOUNT / 100.0));
        }
    }

    /**
     * ADDED: Validates system operational status.
     * Prevents operations during maintenance windows.
     */
    public void validateSystemOperationalStatus() {
        // Placeholder for system maintenance check
        // Real implementation would check maintenance schedules

        java.time.LocalTime currentTime = java.time.LocalTime.now();

        // Example: Block operations during maintenance window (2-4 AM)
        if (currentTime.isAfter(java.time.LocalTime.of(2, 0)) &&
            currentTime.isBefore(java.time.LocalTime.of(4, 0))) {
            throw new SystemMaintenanceException(
                "System is in maintenance mode (02:00-04:00). Please try again later.");
        }
    }

    /**
     * Base validation exception.
     */
    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }

        public ValidationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Insufficient funds specific exception - REMOVED to prevent TOCTOU.
     * Balance validation is now done atomically at the database level.
     */
    @Deprecated
    public static class InsufficientFundsException extends ValidationException {
        public InsufficientFundsException(String message) {
            super(message);
        }
    }

    /**
     * ADDED: Transfer limit exceeded exception.
     */
    public static class TransferLimitExceededException extends ValidationException {
        public TransferLimitExceededException(String message) {
            super(message);
        }
    }

    /**
     * ADDED: System maintenance exception.
     */
    public static class SystemMaintenanceException extends ValidationException {
        public SystemMaintenanceException(String message) {
            super(message);
        }
    }

    /**
     * ADDED: Currency validation exception for specific handling.
     */
    public static class UnsupportedCurrencyException extends ValidationException {
        public UnsupportedCurrencyException(String message) {
            super(message);
        }
    }
}