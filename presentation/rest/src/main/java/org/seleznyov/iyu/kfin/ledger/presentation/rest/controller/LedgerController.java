package org.seleznyov.iyu.kfin.ledger.presentation.rest.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.request.TransferRequestDto;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response.TransferResponseDto;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.mapper.TransferMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * REST controller for ledger transfer operations.
 * Provides endpoints for money transfers between accounts.
 */
@RestController
@RequestMapping("/api/v1/ledger")
@RequiredArgsConstructor
@Validated
@Slf4j
public class LedgerController {

    private final ProcessTransferUseCase processTransferUseCase;
    private final TransferMapper transferMapper;

    /**
     * Processes transfer between accounts.
     *
     * @param request Transfer request parameters
     * @return Transfer operation result
     */
    @PostMapping("/transfers")
    public ResponseEntity<TransferResponseDto> processTransfer(
        @Valid @RequestBody TransferRequestDto request
    ) {
        log.info("Processing transfer request: {} -> {}, amount: {}",
            request.debitAccountId(), request.creditAccountId(), request.amount());

        try {
            // Additional validation
            if (!request.isValid()) {
                log.warn("Invalid transfer request: debit and credit accounts are the same");
                return ResponseEntity.badRequest()
                    .body(TransferResponseDto.failure(
                        null, request.idempotencyKey(),
                        "VALIDATION_ERROR",
                        "Debit and credit accounts must be different",
                        0L
                    ));
            }

            long startTime = System.currentTimeMillis();

            // Convert DTO to use case request
            var useCaseRequest = transferMapper.toUseCaseRequest(request);

            // Process transfer
            var result = processTransferUseCase.processTransfer(useCaseRequest);

            long processingTime = System.currentTimeMillis() - startTime;

            // Convert result to response DTO
            var responseDto = transferMapper.toResponseDto(result, processingTime);

            if (result.success()) {
                log.info("Transfer completed successfully: tx={}", result.transactionId());
                return ResponseEntity.ok(responseDto);
            } else {
                log.warn("Transfer failed: tx={}, reason={}", result.transactionId(), result.failureReason());
                return ResponseEntity.status(determineHttpStatus(result.failureReason()))
                    .body(responseDto);
            }

        } catch (Exception e) {
            log.error("Unexpected error processing transfer: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(TransferResponseDto.failure(
                    null, request.idempotencyKey(),
                    "SYSTEM_ERROR",
                    "Internal server error occurred",
                    0L
                ));
        }
    }

    /**
     * Gets transfer status by transaction ID (for async operations).
     *
     * @param transactionId Transaction ID to check
     * @return Transfer status
     */
    @GetMapping("/transfers/{transactionId}")
    public ResponseEntity<TransferResponseDto> getTransferStatus(
        @PathVariable UUID transactionId
    ) {
        log.debug("Getting transfer status for transaction: {}", transactionId);

        // TODO: Implement transfer status lookup
        // For now, return not implemented
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
            .body(TransferResponseDto.failure(
                transactionId, null,
                "NOT_IMPLEMENTED",
                "Transfer status lookup not yet implemented",
                0L
            ));
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Ledger service is healthy");
    }

    private HttpStatus determineHttpStatus(ProcessTransferUseCase.TransferResult.FailureReason failureReason) {
        return switch (failureReason) {
            case INSUFFICIENT_FUNDS -> HttpStatus.UNPROCESSABLE_ENTITY; // 422
            case VALIDATION_ERROR -> HttpStatus.BAD_REQUEST; // 400
            case IDEMPOTENCY_VIOLATION -> HttpStatus.CONFLICT; // 409
            case ACCOUNT_LOCKED -> HttpStatus.LOCKED; // 423
            case SYSTEM_ERROR -> HttpStatus.INTERNAL_SERVER_ERROR; // 500
        };
    }
}