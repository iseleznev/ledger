package org.seleznyov.iyu.kfin.ledger.presentation.rest.mapper;

import org.seleznyov.iyu.kfin.ledger.presentation.rest.dto.response.BalanceResponseDto;
import org.springframework.stereotype.Component;

/**
 * Mapper for converting balance DTOs to use case objects and vice versa.
 */
@Component
public class BalanceMapper {

    /**
     * Converts use case result to balance response DTO.
     */
    public BalanceResponseDto toResponseDto(GetBalanceUseCase.BalanceResult result, long queryTimeMs) {
        if (result.success()) {
            return BalanceResponseDto.success(
                result.accountId(),
                result.balance(),
                result.operationDate(),
                "ADAPTIVE", // Strategy determined by system
                queryTimeMs
            );
        } else {
            return BalanceResponseDto.failure(
                result.accountId(),
                result.operationDate(),
                result.failureReason().name(),
                result.message(),
                queryTimeMs
            );
        }
    }
}