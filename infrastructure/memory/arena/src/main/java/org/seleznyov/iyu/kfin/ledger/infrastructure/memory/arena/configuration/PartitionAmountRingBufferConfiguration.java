package org.seleznyov.iyu.kfin.ledger.infrastructure.memory.arena.configuration;

public record PartitionAmountRingBufferConfiguration(
    long recordsCount,
    int writeAttempts,
    int writeSpinAttempts,
    int writeYieldAttempts
) {

}
