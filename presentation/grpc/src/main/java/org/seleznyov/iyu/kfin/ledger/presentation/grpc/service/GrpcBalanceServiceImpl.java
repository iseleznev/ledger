package org.seleznyov.iyu.kfin.ledger.presentation.grpc.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper.GrpcBalanceMapper;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.proto.*;

import java.time.LocalDate;
import java.util.UUID;

/**
 * gRPC implementation of BalanceService.
 * Provides high-performance balance operations via gRPC protocol.
 */
@GrpcService
@RequiredArgsConstructor
@Slf4j
public class GrpcBalanceServiceImpl extends BalanceServiceGrpc.BalanceServiceImplBase {

    private final GetBalanceUseCase getBalanceUseCase;
    private final GrpcBalanceMapper grpcBalanceMapper;

    @Override
    public void getBalance(BalanceRequest request, StreamObserver<BalanceResponse> responseObserver) {
        log.debug("gRPC GetBalance: account={}, date={}", request.getAccountId(), request.getOperationDate());

        try {
            long startTime = System.currentTimeMillis();

            // Convert gRPC request to use case request
            var useCaseRequest = grpcBalanceMapper.toUseCaseRequest(request);

            // Get balance
            var result = getBalanceUseCase.getBalance(useCaseRequest);

            long queryTime = System.currentTimeMillis() - startTime;

            // Convert result to gRPC response
            var grpcResponse = grpcBalanceMapper.toGrpcResponse(result, queryTime);

            responseObserver.onNext(grpcResponse);
            responseObserver.onCompleted();

            if (result.success()) {
                log.debug("gRPC balance retrieved successfully: account={}, balance={}",
                    result.accountId(), result.balance());
            } else {
                log.warn("gRPC balance inquiry failed: account={}, reason={}",
                    result.accountId(), result.failureReason());
            }

        } catch (Exception e) {
            log.error("gRPC balance error for account {}: {}", request.getAccountId(), e.getMessage(), e);

            BalanceResponse errorResponse = BalanceResponse.newBuilder()
                .setAccountId(request.getAccountId())
                .setOperationDate(request.getOperationDate())
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
    public void getDetailedBalance(BalanceRequest request, StreamObserver<DetailedBalanceResponse> responseObserver) {
        log.debug("gRPC GetDetailedBalance: account={}, date={}", request.getAccountId(), request.getOperationDate());

        try {
            long startTime = System.currentTimeMillis();

            // Convert gRPC request to use case request
            var useCaseRequest = grpcBalanceMapper.toUseCaseRequest(request);

            // Get detailed balance
            var result = getBalanceUseCase.getDetailedBalance(useCaseRequest);

            long queryTime = System.currentTimeMillis() - startTime;

            // Convert result to gRPC response
            var grpcResponse = grpcBalanceMapper.toDetailedGrpcResponse(result, queryTime);

            responseObserver.onNext(grpcResponse);
            responseObserver.onCompleted();

            if (result.success()) {
                log.debug("gRPC detailed balance retrieved successfully: account={}", result.accountId());
            } else {
                log.warn("gRPC detailed balance inquiry failed: account={}, reason={}",
                    result.accountId(), result.failureReason());
            }

        } catch (Exception e) {
            log.error("gRPC detailed balance error for account {}: {}", request.getAccountId(), e.getMessage(), e);

            DetailedBalanceResponse errorResponse = DetailedBalanceResponse.newBuilder()
                .setAccountId(request.getAccountId())
                .setOperationDate(request.getOperationDate())
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
    public void getBatchBalance(BatchBalanceRequest request, StreamObserver<BatchBalanceResponse> responseObserver) {
        log.debug("gRPC GetBatchBalance: {} accounts, date={}",
            request.getAccountIdsCount(), request.getOperationDate());

        try {
            long startTime = System.currentTimeMillis();

            BatchBalanceResponse.Builder responseBuilder = BatchBalanceResponse.newBuilder()
                .setTotalAccounts(request.getAccountIdsCount())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000)
                    .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                    .build());

            // Process each account
            for (String accountIdStr : request.getAccountIdsList()) {
                try {
                    UUID accountId = UUID.fromString(accountIdStr);
                    LocalDate operationDate = request.getOperationDate().isEmpty() ?
                        LocalDate.now() : LocalDate.parse(request.getOperationDate());

                    var useCaseRequest = new GetBalanceUseCase.BalanceRequest(accountId, operationDate);
                    var result = getBalanceUseCase.getBalance(useCaseRequest);

                    BalanceResult.Builder resultBuilder = BalanceResult.newBuilder()
                        .setAccountId(accountIdStr)
                        .setSuccess(result.success());

                    if (result.success()) {
                        resultBuilder.setBalance(result.balance())
                            .setMessage(result.message());
                    } else {
                        resultBuilder.setMessage("Error: " + result.message());
                    }

                    responseBuilder.addResults(resultBuilder.build());

                } catch (Exception e) {
                    log.warn("Error in batch balance for account {}: {}", accountIdStr, e.getMessage());
                    responseBuilder.addResults(BalanceResult.newBuilder()
                        .setAccountId(accountIdStr)
                        .setSuccess(false)
                        .setMessage("Error: " + e.getMessage())
                        .build());
                }
            }

            long queryTime = System.currentTimeMillis() - startTime;
            responseBuilder.setQueryTimeMs(queryTime);

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

            log.debug("gRPC batch balance completed: {} accounts processed", request.getAccountIdsCount());

        } catch (Exception e) {
            log.error("gRPC batch balance error: {}", e.getMessage(), e);

            BatchBalanceResponse errorResponse = BatchBalanceResponse.newBuilder()
                .setTotalAccounts(request.getAccountIdsCount())
                .setQueryTimeMs(0)
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000)
                    .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                    .build())
                .build();

            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
}