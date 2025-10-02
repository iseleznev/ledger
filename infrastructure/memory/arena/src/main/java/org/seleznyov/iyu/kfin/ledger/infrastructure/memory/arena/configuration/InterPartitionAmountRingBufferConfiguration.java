package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record InterPartitionAmountRingBufferConfiguration(
    long recordsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
