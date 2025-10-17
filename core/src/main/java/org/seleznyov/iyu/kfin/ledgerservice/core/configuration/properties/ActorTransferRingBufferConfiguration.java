package org.seleznyov.iyu.kfin.ledgerservice.core.configuration.properties;

public record ActorTransferRingBufferConfiguration(
    long transfersCount,
    int eventLoopMaxIdleSpins,
    int eventLoopParkNanos,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
