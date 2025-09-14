package org.seleznyov.iyu.kfin.ledger.application.service.arenaseparated;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

// REST Controller для мониторинга и администрирования
@RestController
@RequestMapping("/api/ledger")
@RequiredArgsConstructor
@Slf4j
public class LedgerMonitoringController {

    private final CompleteHybridLedgerService ledgerService;

    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getStatistics() {
        return ResponseEntity.ok(ledgerService.getSystemStatistics());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        return ResponseEntity.ok(ledgerService.getHealthStatus());
    }

    @GetMapping("/balance/{accountId}")
    public ResponseEntity<Map<String, Object>> getBalance(@PathVariable UUID accountId) {
        try {
            long balance = ledgerService.getCurrentBalance(accountId);
            return ResponseEntity.ok(Map.of(
                "accountId", accountId,
                "balance", balance,
                "timestamp", Instant.now()
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", e.getMessage(),
                "accountId", accountId
            ));
        }
    }

    @PostMapping("/admin/cleanup")
    public ResponseEntity<Map<String, Object>> forceCleanup(
        @RequestParam(defaultValue = "30") int inactiveMinutes) {
        try {
            Duration threshold = Duration.ofMinutes(inactiveMinutes);
            Map<String, Object> result = ledgerService.cleanupInactiveAccounts(threshold);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of(
                "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/partition/{accountId}")
    public ResponseEntity<Map<String, Object>> getPartitionStatistics(@PathVariable UUID accountId) {
        try {
            Map<String, Object> stats = ledgerService.getPartitionStatistics(accountId);
            return ResponseEntity.ok(stats);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", e.getMessage(),
                "accountId", accountId
            ));
        }
    }
}
