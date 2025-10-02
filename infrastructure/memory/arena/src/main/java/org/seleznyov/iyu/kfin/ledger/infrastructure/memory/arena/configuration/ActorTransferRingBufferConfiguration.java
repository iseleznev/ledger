package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record ActorTransferRingBufferConfiguration(
    long transfersCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
