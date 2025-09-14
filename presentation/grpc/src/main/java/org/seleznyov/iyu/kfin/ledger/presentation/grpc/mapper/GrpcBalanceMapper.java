package org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper;

import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Mapper for converting gRPC balance messages to use case objects and vice versa.
 */
@Component
public class GrpcBalanceMapper {

    /**
     * Converts gRPC balance request to use case request.
     */
    public GetBalanceUseCase.BalanceRequest toUseCaseRequest(BalanceRequest grpcRequest) {
        UUID accountId = UUID.fromString(grpcRequest.getAccountId());
        LocalDate operationDate = grpcRequest.getOperationDate().isEmpty() ?
            LocalDate.now() : LocalDate.parse(grpcRequest.getOperationDate());

        return new GetBalanceUseCase.BalanceRequest(accountId, operationDate);
    }

    /**
     * Converts use case result to gRPC balance response.
     */
    public BalanceResponse toGrpcResponse(GetBalanceUseCase.BalanceResult result, long queryTimeMs) {
        BalanceResponse.Builder responseBuilder = BalanceResponse.newBuilder()
            .setAccountId(result.accountId().toString())
            .setOperationDate(result.operationDate().toString())
            .setSuccess(result.success())
            .setMessage(result.message())
            .setQueryTimeMs(queryTimeMs)
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build());

        if (result.success()) {
            responseBuilder.setBalance(result.balance())
                .setProcessingStrategy("ADAPTIVE");
        } else {
            responseBuilder.setFailureReason(result.failureReason().name());
        }

        return responseBuilder.build();
    }

    /**
     * Converts detailed balance result to gRPC detailed balance response.
     */
    public DetailedBalanceResponse toDetailedGrpcResponse(GetBalanceUseCase.DetailedBalanceResult result, long queryTimeMs) {
        DetailedBalanceResponse.Builder responseBuilder = DetailedBalanceResponse.newBuilder()
            .setAccountId(result.accountId().toString())
            .setOperationDate(result.operationDate().toString())
            .setSuccess(result.success())
            .setMessage(result.message())
            .setQueryTimeMs(queryTimeMs)
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build());

        if (result.success() && result.breakdown() != null) {
            BalanceBreakdown breakdown = BalanceBreakdown.newBuilder()
                .setCurrentBalance(result.breakdown().currentBalance())
                .setSnapshotBalance(result.breakdown().snapshotBalance())
                .setDeltaFromSnapshot(result.breakdown().deltaFromSnapshot())
                .setOperationsCount(result.breakdown().operationsCount())
                .setSnapshotDate(result.breakdown().snapshotDate() != null ?
                    result.breakdown().snapshotDate().toString() : "")
                .setHasSnapshot(result.breakdown().hasSnapshot())
                .build();

            responseBuilder.setBreakdown(breakdown);
        } else if (!result.success()) {
            responseBuilder.setFailureReason(result.failureReason().name());
        }

        return responseBuilder.build();
    }
}