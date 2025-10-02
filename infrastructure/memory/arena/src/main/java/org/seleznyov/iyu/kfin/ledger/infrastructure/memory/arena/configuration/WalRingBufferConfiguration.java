package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record WalRingBufferConfiguration(
    long entriesCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
