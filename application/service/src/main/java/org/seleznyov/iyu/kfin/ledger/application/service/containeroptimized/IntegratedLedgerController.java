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
 * REST Controller for Integrated Container Optimized Ledger Service
 * Provides high-performance APIs with database persistence
 */
@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
@Slf4j
public class IntegratedLedgerController {

    private final ContainerOptimizedLedgerService ledgerService;

    /**
     * Process double-entry transaction with persistence
     */
    @PostMapping("/transactions/double-entry")
    public CompletableFuture<ResponseEntity<LedgerOperationResult>> processDoubleEntry(
        @Valid @RequestBody DoubleEntryRequest request) {

        log.debug("Processing integrated double-entry: debit={}, credit={}, amount={}, durability={}",
            request.getDebitAccountId(), request.getCreditAccountId(),
            request.getAmount(), request.getDurabilityLevel());

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
     * Get account balance with hybrid memory/database lookup
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
                response.put("source", "hybrid_memory_database");

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
                response.put("source", "hybrid_memory_database");

                return ResponseEntity.ok(response);
            })
            .exceptionally(error -> {
                log.error("Failed to get balances for {} accounts", request.getAccountIds().size(), error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
            });
    }

    /**
     * Reconcile in-memory and database balances
     */
    @PostMapping("/admin/reconcile")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> reconcileBalances(
        @RequestBody @Valid BalancesRequest request) {

        return ledgerService.reconcileBalances(request.getAccountIds())
            .thenApply(result -> {
                Map<String, Object> response = new HashMap<>();
                response.put("totalAccounts", result.getTotalAccounts());
                response.put("discrepancyCount", result.getDiscrepancyCount());
                response.put("accuracyRate", String.format("%.2f%%", result.getAccuracyRate()));
                response.put("hasDiscrepancies", result.hasDiscrepancies());
                response.put("timestamp", System.currentTimeMillis());

                if (result.hasDiscrepancies()) {
                    response.put("discrepancies", result.getDiscrepancies().stream()
                        .limit(100) // Limit response size
                        .map(d -> Map.of(
                            "accountId", d.getAccountId(),
                            "memoryBalance", d.getMemoryBalance(),
                            "databaseBalance", d.getDatabaseBalance(),
                            "difference", d.getDifference()
                        ))
                        .toList());
                }

                return ResponseEntity.ok(response);
            })
            .exceptionally(error -> {
                log.error("Failed to reconcile balances", error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", error.getMessage()));
            });
    }

    /**
     * System health check with database connectivity
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        boolean isHealthy = ledgerService.isSystemHealthy();
        Map<String, Object> health = new HashMap<>();
        health.put("status", isHealthy ? "UP" : "DOWN");
        health.put("timestamp", System.currentTimeMillis());
        health.put("components", "memory_storage,database_persistence,wal");

        if (!isHealthy) {
            health.put("details", ledgerService.getSystemStatistics());
        }

        return ResponseEntity.status(isHealthy ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE)
            .body(health);
    }

    /**
     * Enhanced system statistics including database metrics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStatistics() {
        Map<String, Object> stats = ledgerService.getSystemStatistics();
        stats.put("architecture", "integrated_memory_database");
        stats.put("persistence", "postgresql_immutable");
        return ResponseEntity.ok(stats);
    }

    /**
     * Create full system snapshot (memory + database)
     */
    @PostMapping("/admin/snapshot")
    public CompletableFuture<ResponseEntity<Map<String, Object>>> createSnapshot() {
        return ledgerService.createFullSystemSnapshot()
            .thenApply(ResponseEntity::ok)
            .exceptionally(error -> {
                log.error("Failed to create integrated system snapshot", error);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", error.getMessage()));
            });
    }

    /**
     * Get WAL statistics and status
     */
    @GetMapping("/admin/wal/stats")
    public ResponseEntity<Map<String, Object>> getWalStatistics() {
        // Would integrate with WAL service to get detailed stats
        Map<String, Object> walStats = new HashMap<>();
        walStats.put("message", "WAL statistics available in main /stats endpoint");
        walStats.put("architecture", "integrated_memory_database_wal");
        return ResponseEntity.ok(walStats);
    }

    // Request DTOs (reusing from previous controller)
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

    public static class BalancesRequest {
        @NotEmpty
        @Size(max = 1000)
        private Set<UUID> accountIds;

        public Set<UUID> getAccountIds() { return accountIds; }
        public void setAccountIds(Set<UUID> accountIds) { this.accountIds = accountIds; }
    }
}