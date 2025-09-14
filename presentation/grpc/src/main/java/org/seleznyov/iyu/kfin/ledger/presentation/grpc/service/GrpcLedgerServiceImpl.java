package org.seleznyov.iyu.kfin.ledger.presentation.grpc.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper.GrpcLedgerMapper;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.proto.*;

/**
 * gRPC implementation of LedgerService.
 * Provides high-performance transfer operations via gRPC protocol.
 */
@GrpcService
@RequiredArgsConstructor
@Slf4j
public class GrpcLedgerServiceImpl extends LedgerServiceGrpc.LedgerServiceImplBase {

    private final ProcessTransferUseCase processTransferUseCase;
    private final GrpcLedgerMapper grpcLedgerMapper;

    @Override
    public void processTransfer(TransferRequest request, StreamObserver<TransferResponse> responseObserver) {
        log.debug("gRPC ProcessTransfer: {} -> {}, amount: {}",
            request.getDebitAccountId(), request.getCreditAccountId(), request.getAmount());

        try {
            // Validate request
            if (request.getDebitAccountId().equals(request.getCreditAccountId())) {
                TransferResponse errorResponse = TransferResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Debit and credit accounts must be different")
                    .setFailureReason("VALIDATION_ERROR")
                    .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(System.currentTimeMillis() / 1000)
                        .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                        .build())
                    .build();

                responseObserver.onNext(errorResponse);
                responseObserver.onCompleted();
                return;
            }

            long startTime = System.currentTimeMillis();

            // Convert gRPC request to use case request
            var useCaseRequest = grpcLedgerMapper.toUseCaseRequest(request);

            // Process transfer
            var result = processTransferUseCase.processTransfer(useCaseRequest);

            long processingTime = System.currentTimeMillis() - startTime;

            // Convert result to gRPC response
            var grpcResponse = grpcLedgerMapper.toGrpcResponse(result, processingTime);

            responseObserver.onNext(grpcResponse);
            responseObserver.onCompleted();

            if (result.success()) {
                log.info("gRPC transfer completed successfully: tx={}", result.transactionId());
            } else {
                log.warn("gRPC transfer failed: tx={}, reason={}", result.transactionId(), result.failureReason());
            }

        } catch (Exception e) {
            log.error("gRPC transfer error: {}", e.getMessage(), e);

            TransferResponse errorResponse = TransferResponse.newBuilder()
                .setSuccess(false)
                .setMessage("Internal server error: " + e.getMessage())
                .setFailureReason("SYSTEM_ERROR")
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000)
                    .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                    .build())
                .build();

            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void getTransferStatus(TransferStatusRequest request, StreamObserver<TransferResponse> responseObserver) {
        log.debug("gRPC GetTransferStatus: tx={}", request.getTransactionId());

        // TODO: Implement transfer status lookup
        TransferResponse response = TransferResponse.newBuilder()
            .setTransactionId(request.getTransactionId())
            .setSuccess(false)
            .setMessage("Transfer status lookup not yet implemented")
            .setFailureReason("NOT_IMPLEMENTED")
            .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build())
            .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void streamTransferEvents(TransferEventRequest request, StreamObserver<TransferEvent> responseObserver) {
        log.debug("gRPC StreamTransferEvents: accounts={}, includeSystemEvents={}",
            request.getAccountIdsList(), request.getIncludeSystemEvents());

        // TODO: Implement event streaming
        // For now, just complete the stream
        responseObserver.onCompleted();
    }
}