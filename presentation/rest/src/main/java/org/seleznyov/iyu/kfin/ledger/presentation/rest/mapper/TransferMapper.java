package org.seleznyov.iyu.kfin.ledger.presentation.rest.mapper;

import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.request.TransferRequestDto;
import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response.TransferResponseDto;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Mapper for converting transfer DTOs to use case objects and vice versa.
 */
@Component
public class TransferMapper {

    /**
     * Converts transfer request DTO to use case request.
     */
    public ProcessTransferUseCase.TransferRequest toUseCaseRequest(TransferRequestDto dto) {
        return new ProcessTransferUseCase.TransferRequest(
            dto.debitAccountId(),
            dto.creditAccountId(),
            dto.amount(),
            dto.currencyCode(),
            dto.operationDate() != null ? dto.operationDate() : LocalDate.now(),
            UUID.randomUUID(), // Generate transaction ID
            dto.idempotencyKey() != null ? dto.idempotencyKey() : UUID.randomUUID()
        );
    }

    /**
     * Converts use case result to transfer response DTO.
     */
    public TransferResponseDto toResponseDto(ProcessTransferUseCase.TransferResult result, long processingTimeMs) {
        if (result.success()) {
            return TransferResponseDto.success(
                result.transactionId(),
                result.idempotencyKey(),
                result.message(),
                "ADAPTIVE", // Strategy determined by system
                processingTimeMs
            );
        } else {
            return TransferResponseDto.failure(
                result.transactionId(),
                result.idempotencyKey(),
                result.failureReason().name(),
                result.message(),
                processingTimeMs
            );
        }
    }
}