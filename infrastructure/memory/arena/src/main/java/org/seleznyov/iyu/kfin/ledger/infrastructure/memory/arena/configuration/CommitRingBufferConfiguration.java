package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record CommitRingBufferConfiguration(
    long commitsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
