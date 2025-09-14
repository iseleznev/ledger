package org.seleznyov.iyu.kfin.ledger.application.usecase;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.application.service.snapshot.AdaptiveSnapshotService;
import org.seleznyov.iyu.kfin.ledger.domain.service.ValidationService;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Use case for creating account balance snapshots.
 * Coordinates validation and delegates to appropriate snapshot service strategy.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CreateSnapshotUseCase {

    private final ValidationService validationService;
    private final AdaptiveSnapshotService adaptiveSnapshotService;

    /**
     * Creates balance snapshot for account as of specified date.
     *
     * @param request Snapshot creation request
     * @return Snapshot creation result
     */
    @Transactional
    public SnapshotResult createSnapshot(SnapshotRequest request) {
        log.debug("Creating snapshot for account: {}, date: {}",
            request.accountId(), request.operationDate());

        try {
            // Validate request parameters
            validationService.validateSnapshotCreation(
                request.accountId(),
                request.operationDate()
            );

            // Delegate to adaptive snapshot service (will choose appropriate strategy)
            var snapshot = adaptiveSnapshotService.createSnapshot(
                request.accountId(),
                request.operationDate()
            );

            log.info("Snapshot created successfully for account {}: balance={}, entries={}",
                request.accountId(), snapshot.balance(), snapshot.operationsCount());

            return SnapshotResult.success(
                request.accountId(),
                request.operationDate(),
                snapshot.balance(),
                snapshot.operationsCount(),
                "Snapshot created successfully"
            );

        } catch (ValidationService.ValidationException e) {
            log.warn("Snapshot creation failed - validation error: {}", e.getMessage());
            return SnapshotResult.failure(
                request.accountId(),
                request.operationDate(),
                SnapshotResult.FailureReason.VALIDATION_ERROR,
                e.getMessage()
            );

        } catch (IllegalStateException e) {
            log.warn("Snapshot creation failed - already exists: {}", e.getMessage());
            return SnapshotResult.failure(
                request.accountId(),
                request.operationDate(),
                SnapshotResult.FailureReason.ALREADY_EXISTS,
                e.getMessage()
            );

        } catch (Exception e) {
            log.error("Snapshot creation failed - system error: {}", e.getMessage(), e);
            return SnapshotResult.failure(
                request.accountId(),
                request.operationDate(),
                SnapshotResult.FailureReason.SYSTEM_ERROR,
                "Internal system error occurred"
            );
        }
    }

    /**
     * Creates snapshots for multiple accounts (batch operation).
     *
     * @param request Batch snapshot creation request
     * @return Batch snapshot creation result
     */
    @Transactional
    public BatchSnapshotResult createBatchSnapshot(BatchSnapshotRequest request) {
        log.debug("Creating batch snapshots for {} accounts, date: {}",
            request.accountIds().size(), request.operationDate());

        int successful = 0;
        int failed = 0;

        for (UUID accountId : request.accountIds()) {
            try {
                var singleRequest = new SnapshotRequest(accountId, request.operationDate());
                var result = createSnapshot(singleRequest);

                if (result.success()) {
                    successful++;
                } else {
                    failed++;
                    log.warn("Batch snapshot failed for account {}: {}",
                        accountId, result.message());
                }

            } catch (Exception e) {
                failed++;
                log.error("Batch snapshot error for account {}: {}", accountId, e.getMessage(), e);
            }
        }

        log.info("Batch snapshot completed: {} successful, {} failed", successful, failed);

        return new BatchSnapshotResult(
            request.accountIds().size(),
            successful,
            failed,
            request.operationDate(),
            successful > 0,
            "Batch snapshot completed"
        );
    }

    /**
     * Snapshot creation request parameters.
     */
    public record SnapshotRequest(
        UUID accountId,
        LocalDate operationDate
    ) {

        /**
         * Creates snapshot request for current date.
         */
        public static SnapshotRequest of(UUID accountId) {
            return new SnapshotRequest(accountId, LocalDate.now());
        }
    }

    /**
     * Batch snapshot creation request parameters.
     */
    public record BatchSnapshotRequest(
        java.util.List<UUID> accountIds,
        LocalDate operationDate
    ) {

        /**
         * Creates batch snapshot request for current date.
         */
        public static BatchSnapshotRequest of(java.util.List<UUID> accountIds) {
            return new BatchSnapshotRequest(accountIds, LocalDate.now());
        }
    }

    /**
     * Snapshot creation result.
     */
    public record SnapshotResult(
        UUID accountId,
        LocalDate operationDate,
        Long balance,
        Integer operationsCount,
        boolean success,
        FailureReason failureReason,
        String message,
        long processingTimeMs
    ) {

        public static SnapshotResult success(
            UUID accountId,
            LocalDate operationDate,
            long balance,
            int operationsCount,
            String message
        ) {
            return new SnapshotResult(
                accountId,
                operationDate,
                balance,
                operationsCount,
                true,
                null,
                message,
                System.currentTimeMillis()
            );
        }

        public static SnapshotResult failure(
            UUID accountId,
            LocalDate operationDate,
            FailureReason reason,
            String message
        ) {
            return new SnapshotResult(
                accountId,
                operationDate,
                null,
                null,
                false,
                reason,
                message,
                System.currentTimeMillis()
            );
        }

        public enum FailureReason {
            VALIDATION_ERROR,
            ALREADY_EXISTS,
            ACCOUNT_NOT_FOUND,
            INSUFFICIENT_DATA,
            SYSTEM_ERROR
        }
    }

    /**
     * Batch snapshot creation result.
     */
    public record BatchSnapshotResult(
        int totalAccounts,
        int successfulSnapshots,
        int failedSnapshots,
        LocalDate operationDate,
        boolean success,
        String message
    ) {

    }
}