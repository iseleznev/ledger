package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record PostgresRingBufferConfiguration(
    long entriesCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
