package org.seleznyov.iyu.kfin.ledger.presentation.grpc.service;

import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.seleznyov.iyu.kfin.ledger.application.service.snapshot.AdaptiveSnapshotService;
import org.seleznyov.iyu.kfin.ledger.application.service.AdaptiveStrategyService;
import org.seleznyov.iyu.kfin.ledger.application.service.LedgerService;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.mapper.GrpcAdminMapper;
import org.seleznyov.iyu.kfin.ledger.presentation.grpc.proto.*;

import java.util.UUID;

/**
 * gRPC implementation of AdminService.
 * Provides system monitoring and management via gRPC protocol.
 */
@GrpcService
@RequiredArgsConstructor
@Slf4j
public class GrpcAdminServiceImpl extends AdminServiceGrpc.AdminServiceImplBase {

    private final LedgerService ledgerService;
    private final AdaptiveStrategyService adaptiveStrategyService;
    private final AdaptiveSnapshotService adaptiveSnapshotService;
    private final CreateSnapshotUseCase createSnapshotUseCase;
    private final MLEnhancedHotAccountDetector hotAccountDetector;
    private final SlidingWindowMetrics slidingWindowMetrics;
    private final GrpcAdminMapper grpcAdminMapper;

    @Override
    public void getSystemStats(SystemStatsRequest request, StreamObserver<SystemStatsResponse> responseObserver) {
        log.debug("gRPC GetSystemStats");

        try {
            var ledgerStats = ledgerService.getProcessingStats();
            var strategyStats = adaptiveStrategyService.getStrategyStats();
            var snapshotStats = adaptiveSnapshotService.getSnapshotStats();
            var detectionStats = hotAccountDetector.getDetectionStats();
            int trackedAccounts = slidingWindowMetrics.getTrackedAccountsCount();

            var response = grpcAdminMapper.buildSystemStatsResponse(
                ledgerStats, strategyStats, snapshotStats, detectionStats, trackedAccounts
            );

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.debug("gRPC system stats retrieved successfully");

        } catch (Exception e) {
            log.error("gRPC system stats error: {}", e.getMessage(), e);

            SystemStatsResponse errorResponse = SystemStatsResponse.newBuilder()
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
    public void getAccountAnalysis(AccountAnalysisRequest request, StreamObserver<AccountAnalysisResponse> responseObserver) {
        log.debug("gRPC GetAccountAnalysis: account={}", request.getAccountId());

        try {
            UUID accountId = UUID.fromString(request.getAccountId());

            var strategyAnalysis = adaptiveStrategyService.getDetailedAnalysis(accountId);
            var hotAccountAnalysis = hotAccountDetector.getDetailedAnalysis(accountId);
            var metricsSnapshot = slidingWindowMetrics.getMetrics(accountId);

            var response = grpcAdminMapper.buildAccountAnalysisResponse(
                accountId, strategyAnalysis, hotAccountAnalysis, metricsSnapshot
            );

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            log.debug("gRPC account analysis retrieved successfully for: {}", accountId);

        } catch (IllegalArgumentException e) {
            log.warn("Invalid account ID in gRPC request: {}", request.getAccountId());

            AccountAnalysisResponse errorResponse = AccountAnalysisResponse.newBuilder()
                .setAccountId(request.getAccountId())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000)
                    .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                    .build())
                .build();

            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();

        } catch (Exception e) {
            log.error("gRPC account analysis error for {}: {}", request.getAccountId(), e.getMessage(), e);

            AccountAnalysisResponse errorResponse = AccountAnalysisResponse.newBuilder()
                .setAccountId(request.getAccountId())
                .setTimestamp(com.google.protobuf.Timestamp.newBuilder()
                    .setSeconds(System.currentTimeMillis() / 1000)
                    .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                    .build())
                .build();

            response