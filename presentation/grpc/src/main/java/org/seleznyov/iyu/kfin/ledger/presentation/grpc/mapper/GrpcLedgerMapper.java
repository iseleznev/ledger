package org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper;

import org.seleznyov.iyu.kfin.ledger.presentation.grpc.proto.TransferRequest;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.proto.TransferResponse;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.UUID;

/**
 * Mapper for converting gRPC transfer messages to use case objects and vice versa.
 */
@Component
public class GrpcLedgerMapper {

    /**
     * Converts gRPC transfer request to use case request.
     */
    public ProcessTransferUseCase.TransferRequest toUseCaseRequest(TransferRequest grpcRequest) {
        return new ProcessTransferUseCase.TransferRequest(
            UUID.fromString(grpcRequest.getDebitAccountId()),
            UUID.fromString(grpcRequest.getCreditAccountId()),
            grpcRequest.getAmount(),
            grpcRequest.getCurrencyCode(),
            grpcRequest.getOperationDate().isEmpty() ?
                LocalDate.now() : LocalDate.parse(grpcRequest.getOperationDate()),
            UUID.randomUUID(), // Generate transaction ID
            grpcRequest.getIdempotencyKey().isEmpty() ?
                UUID.randomUUID() : UUID.fromString(grpcRequest.getIdempotencyKey())
        );
    }

    /**
     * Converts use case result to gRPC transfer response.
     */
    public TransferResponse toGrpcResponse(ProcessTransferUseCase.TransferResult result, long processingTimeMs) {
        TransferResponse.Builder responseBuilder = TransferResponse.newBuilder()
            .setTransactionId(result.transactionId().toString())
            .setIdempotencyKey(result.idempotencyKey().toString())
            .setSuccess(result.success())
            .setMessage(result.message())
            .setProcessingTimeMs(processingTimeMs)
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build());

        if (result.success()) {
            responseBuilder.setProcessingStrategy("ADAPTIVE");
        } else {
            responseBuilder.setFailureReason(result.failureReason().name());
        }

        return responseBuilder.build();
    }
}