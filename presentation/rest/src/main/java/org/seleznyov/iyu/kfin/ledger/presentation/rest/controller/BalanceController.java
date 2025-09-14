package org.seleznyov.iyu.kfin.ledger.presentation.rest.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response.BalanceResponseDto;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.mapper.BalanceMapper;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

/**
 * REST controller for account balance operations.
 * Provides endpoints for balance inquiries and detailed balance analysis.
 */
@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
@Validated
@Slf4j
public class BalanceController {

    private final GetBalanceUseCase getBalanceUseCase;
    private final BalanceMapper balanceMapper;

    /**
     * Gets account balance for current date.
     *
     * @param accountId Account identifier
     * @return Balance inquiry result
     */
    @GetMapping("/accounts/{accountId}/balance")
    public ResponseEntity<BalanceResponseDto> getBalance(
        @PathVariable UUID accountId
    ) {
        log.debug("Getting current balance for account: {}", accountId);

        try {
            long startTime = System.currentTimeMillis();

            var request = GetBalanceUseCase.BalanceRequest.of(accountId);
            var result = getBalanceUseCase.getBalance(request);

            long queryTime = System.currentTimeMillis() - startTime;

            var responseDto = balanceMapper.toResponseDto(result, queryTime);

            if (result.success()) {
                log.debug("Balance retrieved successfully for account {}: {}", accountId, result.balance());
                return ResponseEntity.ok(responseDto);
            } else {
                log.warn("Balance inquiry failed for account {}: {}", accountId, result.failureReason());
                return ResponseEntity.status(determineHttpStatus(result.failureReason()))
                    .body(responseDto);
            }

        } catch (Exception e) {
            log.error("Unexpected error getting balance for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(BalanceResponseDto.failure(
                    accountId, LocalDate.now(),
                    "SYSTEM_ERROR",
                    "Internal server error occurred",
                    0L
                ));
        }
    }

    /**
     * Gets account balance for specific date.
     *
     * @param accountId     Account identifier
     * @param operationDate Date for balance calculation
     * @return Balance inquiry result
     */
    @GetMapping("/accounts/{accountId}/balance/{operationDate}")
    public ResponseEntity<BalanceResponseDto> getBalanceForDate(
        @PathVariable UUID accountId,
        @PathVariable @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate operationDate
    ) {
        log.debug("Getting balance for account: {}, date: {}", accountId, operationDate);

        try {
            long startTime = System.currentTimeMillis();

            var request = new GetBalanceUseCase.BalanceRequest(accountId, operationDate);
            var result = getBalanceUseCase.getBalance(request);

            long queryTime = System.currentTimeMillis() - startTime;

            var responseDto = balanceMapper.toResponseDto(result, queryTime);

            if (result.success()) {
                log.debug("Balance retrieved successfully for account {} on {}: {}",
                    accountId, operationDate, result.balance());
                return ResponseEntity.ok(responseDto);
            } else {
                log.warn("Balance inquiry failed for account {} on {}: {}",
                    accountId, operationDate, result.failureReason());
                return ResponseEntity.status(determineHttpStatus(result.failureReason()))
                    .body(responseDto);
            }

        } catch (Exception e) {
            log.error("Unexpected error getting balance for account {} on {}: {}",
                accountId, operationDate, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(BalanceResponseDto.failure(
                    accountId, operationDate,
                    "SYSTEM_ERROR",
                    "Internal server error occurred",
                    0L
                ));
        }
    }

    /**
     * Gets detailed balance analysis with breakdown.
     *
     * @param accountId Account identifier
     * @return Detailed balance analysis
     */
    @GetMapping("/accounts/{accountId}/balance/detailed")
    public ResponseEntity<?> getDetailedBalance(
        @PathVariable UUID accountId,
        @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate operationDate
    ) {
        LocalDate queryDate = operationDate != null ? operationDate : LocalDate.now();
        log.debug("Getting detailed balance for account: {}, date: {}", accountId, queryDate);

        try {
            long startTime = System.currentTimeMillis();

            var request = new GetBalanceUseCase.BalanceRequest(accountId, queryDate);
            var result = getBalanceUseCase.getDetailedBalance(request);

            long queryTime = System.currentTimeMillis() - startTime;

            if (result.success()) {
                log.debug("Detailed balance retrieved successfully for account {}", accountId);
                return ResponseEntity.ok(
                    Map.of(
                        "accountId", accountId,
                        "operationDate", queryDate,
                        "breakdown", result.breakdown(),
                        "success", true,
                        "queryTimeMs", queryTime,
                        "timestamp", java.time.LocalDateTime.now()
                    )
                );
            } else {
                log.warn("Detailed balance inquiry failed for account {}: {}", accountId, result.failureReason());
                return ResponseEntity.status(determineHttpStatus(result.failureReason()))
                    .body(Map.of(
                        "accountId", accountId,
                        "operationDate", queryDate,
                        "success", false,
                        "failureReason", result.failureReason(),
                        "message", result.message(),
                        "queryTimeMs", queryTime,
                        "timestamp", java.time.LocalDateTime.now()
                    ));
            }

        } catch (Exception e) {
            log.error("Unexpected error getting detailed balance for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                    "accountId", accountId,
                    "operationDate", queryDate,
                    "success", false,
                    "failureReason", "SYSTEM_ERROR",
                    "message", "Internal server error occurred",
                    "queryTimeMs", 0L,
                    "timestamp", java.time.LocalDateTime.now()
                ));
        }
    }

    /**
     * Batch balance inquiry for multiple accounts.
     *
     * @param request Batch balance request
     * @return Batch balance results
     */
    @PostMapping("/accounts/balance/batch")
    public ResponseEntity<?> getBatchBalance(
        @Valid @RequestBody BatchBalanceRequest request
    ) {
        log.debug("Getting batch balance for {} accounts", request.accountIds().size());

        try {
            long startTime = System.currentTimeMillis();

            var results = request.accountIds().stream()
                .map(accountId -> {
                    try {
                        var balanceRequest = new GetBalanceUseCase.BalanceRequest(
                            accountId, request.operationDate() != null ? request.operationDate() : LocalDate.now()
                        );
                        var result = getBalanceUseCase.getBalance(balanceRequest);
                        return Map.of(
                            "accountId", accountId,
                            "success", result.success(),
                            "balance", result.balance(),
                            "message", result.message()
                        );
                    } catch (Exception e) {
                        return Map.of(
                            "accountId", accountId,
                            "success", false,
                            "balance", null,
                            "message", "Error: " + e.getMessage()
                        );
                    }
                })
                .toList();

            long queryTime = System.currentTimeMillis() - startTime;

            return ResponseEntity.ok(Map.of(
                "results", results,
                "totalAccounts", request.accountIds().size(),
                "queryTimeMs", queryTime,
                "timestamp", java.time.LocalDateTime.now()
            ));

        } catch (Exception e) {
            log.error("Unexpected error in batch balance inquiry: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of(
                    "success", false,
                    "message", "Internal server error occurred",
                    "timestamp", java.time.LocalDateTime.now()
                ));
        }
    }

    private HttpStatus determineHttpStatus(GetBalanceUseCase.BalanceResult.FailureReason failureReason) {
        return switch (failureReason) {
            case VALIDATION_ERROR -> HttpStatus.BAD_REQUEST;
            case ACCOUNT_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case SYSTEM_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    /**
     * Batch balance request DTO.
     */
    public record BatchBalanceRequest(
        java.util.List<UUID> accountIds,
        LocalDate operationDate
    ) {

    }
}