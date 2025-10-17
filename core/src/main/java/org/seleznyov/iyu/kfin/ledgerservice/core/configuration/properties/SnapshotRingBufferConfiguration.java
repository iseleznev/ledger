package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record SnapshotRingBufferConfiguration(
    long snapshotsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts,
    int eventLoopMaxIdleSpins,
    int eventLoopParkNanos
) {

}
