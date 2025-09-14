package org.seleznyov.iyu.kfin.ledger.application.service.containeroptimized;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * REST Controller for Container Optimized Ledger Service
 */
@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
@Slf4j
public class LedgerController {

    private final ContainerOptimizedLedgerService ledgerService;

    /**
     * Process double-entry transaction
     */
    @PostMapping("/transactions/double-entry")
    public CompletableFuture<ResponseEntity<LedgerOperationResult>> processDoubleEntry(
        @Valid @RequestBody DoubleEntryRequest request) {

        log.debug("Processing double-entry: debit={}, credit={}, amount={}",
            request.getDebitAccountId(), request.getCreditAccountId(), request.getAmount());

        return ledgerService.processDoubleEntry(
                request.getDebitAccountId(),
                request.getCreditAccountId(),
                request.getAmount(),
                request.getCurrencyCode(),
                request.getDurabilityLevel()
            )
            .thenApply(result -> {
                if (result.isSuccess()) {
                    return ResponseEntity.ok(result);
                } else {
                    return ResponseEntity.badRequest().body(result);
                }
            })
            .exceptionally(error -> {
                log.error("Double-entry operation failed", error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(LedgerOperationResult.failure(error.getMessage(), "INTERNAL_ERROR"));
            })
            .orTimeout(30, TimeUnit.SECONDS);
    }

    /**
     * Process single-entry transaction
     */
    @PostMapping("/transactions/single-entry")
    public CompletableFuture<ResponseEntity<LedgerOperationResult>> processSingleEntry(
        @Valid @RequestBody SingleEntryRequest request) {

        log.debug("Processing single-entry: account={}, amount={}, type={}",
            request.getAccountId(), request.getAmount(), request.getEntryType());

        return ledgerService.processSingleEntry(
                request.getAccountId(),
                request.getAmount(),
                request.getCurrencyCode(),
                request.getDurabilityLevel(),
                request.getEntryType()
            )
            .thenApply(result -> {
                if (result.isSuccess()) {
                    return ResponseEntity.ok(result);
                } else {
                    return ResponseEntity.badRequest().body(result);
                }
            })
            .exceptionally(error -> {
                log.error("Single-entry operation failed", error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(LedgerOperationResult.failure(error.getMessage(), "INTERNAL_ERROR"));
            })
            .orTimeout(30, TimeUnit.SECONDS);
    }

    /**
     * Get account balance
     */
    @GetMapping("/accounts/{accountId}/balance")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> getBalance(
        @PathVariable UUID accountId) {

        return ledgerService.getBalance(accountId)
            .thenApply(balance -> {
                Map<String, Object> response = new HashMap<>();
                response.put("accountId", accountId);
                response.put("balance", balance.orElse(0L));
                response.put("hasBalance", balance.isPresent());
                response.put("timestamp", System.currentTimeMillis());

                return ResponseEntity.ok(response);
            })
            .exceptionally(error -> {
                log.error("Failed to get balance for account {}", accountId, error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    /**
     * Get balances for multiple accounts
     */
    @PostMapping("/accounts/balances")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> getBalances(
        @RequestBody @Valid BalancesRequest request) {

        if (request.getAccountIds().isEmpty()) {
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of("error", "Account IDs list cannot be empty")));
        }

        if (request.getAccountIds().size() > 1000) {
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of("error", "Too many account IDs (max 1000)")));
        }

        return ledgerService.getBalances(request.getAccountIds())
            .thenApply(balances -> {
                Map<String, Object> response = new HashMap<>();
                response.put("balances", balances);
                response.put("count", balances.size());
                response.put("timestamp", System.currentTimeMillis());

                return ResponseEntity.ok(response);
            })
            .exceptionally(error -> {
                log.error("Failed to get balances for {} accounts", request.getAccountIds().size(), error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    /**
     * Get balance history with snapshots
     */
    @GetMapping("/accounts/{accountId}/history")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> getBalanceHistory(
        @PathVariable UUID accountId,
        @RequestParam(defaultValue = "7") int days) {

        if (days < 1 || days > 365) {
            return CompletableFuture.completedFuture(
                ResponseEntity.badRequest().body(Map.of("error", "Days must be between 1 and 365")));
        }

        return ledgerService.getBalanceHistory(accountId, days)
            .thenApply(ResponseEntity::ok)
            .exceptionally(error -> {
                log.error("Failed to get balance history for account {}", accountId, error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    /**
     * System health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        boolean isHealthy = ledgerService.isSystemHealthy();
        Map<String, Object> health = new HashMap<>();
        health.put("status", isHealthy ? "UP" : "DOWN");
        health.put("timestamp", System.currentTimeMillis());

        if (!isHealthy) {
            health.put("details", ledgerService.getSystemStatistics());
        }

        return ResponseEntity.status(isHealthy ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
            .body(health);
    }

    /**
     * System statistics and metrics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStatistics() {
        return ResponseEntity.ok(ledgerService.getSystemStatistics());
    }

    /**
     * Create full system snapshot (for backups)
     */
    @PostMapping("/admin/snapshot")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> createSnapshot() {
        return ledgerService.createFullSystemSnapshot()
            .thenApply(ResponseEntity::ok)
            .exceptionally(error -> {
                log.error("Failed to create system snapshot", error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", error.getMessage()));
            });
    }

    // Request DTOs
    public static class DoubleEntryRequest {
        @NotNull
        private UUID debitAccountId;

        @NotNull
        private UUID creditAccountId;

        @Positive
        private long amount;

        @NotBlank
        @Size(min = 3, max = 3)
        private String currencyCode = "USD";

        private DurabilityLevel durabilityLevel = DurabilityLevel.WAL_BUFFERED;

        // Getters and setters
        public UUID getDebitAccountId() { return debitAccountId; }
        public void setDebitAccountId(UUID debitAccountId) { this.debitAccountId = debitAccountId; }

        public UUID getCreditAccountId() { return creditAccountId; }
        public void setCreditAccountId(UUID creditAccountId) { this.creditAccountId = creditAccountId; }

        public long getAmount() { return amount; }
        public void setAmount(long amount) { this.amount = amount; }

        public String getCurrencyCode() { return currencyCode; }
        public void setCurrencyCode(String currencyCode) { this.currencyCode = currencyCode; }

        public DurabilityLevel getDurabilityLevel() { return durabilityLevel; }
        public void setDurabilityLevel(DurabilityLevel durabilityLevel) { this.durabilityLevel = durabilityLevel; }
    }

    public static class SingleEntryRequest {
        @NotNull
        private UUID accountId;

        @NotNull
        private Long amount; // Can be positive or negative

        @NotBlank
        @Size(min = 3, max = 3)
        private String currencyCode = "USD";

        @Pattern(regexp = "DEBIT|CREDIT")
        private String entryType;

        private DurabilityLevel durabilityLevel = DurabilityLevel.WAL_BUFFERED;

        // Getters and setters
        public UUID getAccountId() { return accountId; }
        public void setAccountId(UUID accountId) { this.accountId = accountId; }

        public Long getAmount() { return amount; }
        public void setAmount(Long amount) { this.amount = amount; }

        public String getCurrencyCode() { return currencyCode; }
        public void setCurrencyCode(String currencyCode) { this.currencyCode = currencyCode; }

        public String getEntryType() { return entryType; }
        public void setEntryType(String entryType) { this.entryType = entryType; }

        public DurabilityLevel getDurabilityLevel() { return durabilityLevel; }
        public void setDurabilityLevel(DurabilityLevel durabilityLevel) { this.durabilityLevel = durabilityLevel; }
    }

    public static class BalancesRequest {
        @NotEmpty
        @Size(max = 1000)
        private Set<UUID> accountIds;

        public Set<UUID> getAccountIds() { return accountIds; }
        public void setAccountIds(Set<UUID> accountIds) { this.accountIds = accountIds; }
    }
}